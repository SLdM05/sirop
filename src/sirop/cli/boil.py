"""Handler for ``sirop boil [--from <stage>]``.

Runs the tax calculation pipeline on the active batch, picking up after the
``tap`` stage.  The stages executed are, in order:

    normalize → verify (pass-through) → transfer_match → boil/ACB → superficial_loss

Each stage is skipped if ``stage_status`` is ``done`` and ``--from`` does not
include or precede it.  Downstream stages are invalidated when a stage is
re-run.

Usage
-----
    sirop boil                           # run all pending stages
    sirop boil --from transfer_match     # re-run from transfer_match onward
"""

from __future__ import annotations

import dataclasses
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.db.schema import PIPELINE_STAGES
from sirop.engine import acb as acb_engine
from sirop.engine import superficial_loss as sld_engine
from sirop.engine.acb import TaxRules
from sirop.models.messages import MessageCode
from sirop.normalizer import normalizer
from sirop.transfer_match import matcher
from sirop.utils.boc import BoCRateError, prefetch_rates
from sirop.utils.crypto_prices import prefetch_crypto_prices, prefetch_crypto_prices_by_range
from sirop.utils.logging import StageContext, get_logger
from sirop.utils.messages import emit

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date

    from sirop.models.raw import RawTransaction

logger = get_logger(__name__)

# Stages this command is responsible for (subset of PIPELINE_STAGES).
_BOIL_STAGES = ("normalize", "verify", "transfer_match", "boil", "superficial_loss")

# Path to the built-in tax rules config relative to the working directory.
_TAX_RULES_PATH = Path("config/tax_rules.yaml")


class _BoilError(Exception):
    """Sentinel — raised to exit early; caught by handle_boil."""

    def __init__(self, code: MessageCode, **kwargs: object) -> None:
        self.msg_code = code
        self.msg_kwargs = kwargs
        super().__init__(str(code))


def handle_boil(
    from_stage: str | None,
    settings: Settings | None = None,
) -> int:
    """Run the pipeline from *from_stage* (or from the beginning if None).

    Parameters
    ----------
    from_stage:
        When provided, all stages before this one are skipped even if they
        are pending.  Stages after it (and itself) are re-run even if done.
    settings:
        Application settings; resolved from the environment if omitted.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on any error.
    """
    if settings is None:
        settings = get_settings()
    try:
        return _run_boil(from_stage, settings)
    except _BoilError as exc:
        emit(exc.msg_code, **exc.msg_kwargs)
        return 1


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run_boil(from_stage: str | None, settings: Settings) -> int:
    """Core pipeline coordinator.  Raises ``_BoilError`` on any user-facing error."""
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _BoilError(MessageCode.BATCH_ERROR_NO_ACTIVE)

    conn = open_batch(batch_name, settings)
    try:
        # Ensure tap has been run.
        tap_status = repo.get_stage_status(conn, "tap")
        if tap_status != "done":
            raise _BoilError(MessageCode.BOIL_ERROR_NOT_TAPPED, name=batch_name)

        tax_rules = _load_tax_rules()

        # Invalidate stages that will be re-run and clear their stale output.
        if from_stage is not None:
            _validate_from_stage(from_stage)
            stages_to_invalidate = _stages_from(from_stage)
            repo.set_stages_invalidated(conn, list(stages_to_invalidate))
            repo.clear_stages_output(conn, stages_to_invalidate)

        # Execute each stage in order.
        for stage in _BOIL_STAGES:
            should_run = _should_run(conn, stage, from_stage)
            if not should_run:
                status = repo.get_stage_status(conn, stage)
                logger.info("boil: skipping %s (status=%s)", stage, status)
                continue

            _check_not_running(conn, stage, batch_name)
            _execute_stage(conn, stage, batch_name, tax_rules)

        _print_summary(conn, batch_name)
        return 0

    finally:
        conn.close()


def _execute_stage(
    conn: object,  # sqlite3.Connection — typed generically to avoid circular import hint
    stage: str,
    batch_name: str,
    tax_rules: TaxRules,
) -> None:
    """Execute a single pipeline stage wrapped in StageContext."""
    assert isinstance(conn, sqlite3.Connection)

    with StageContext(batch_id=batch_name, stage=stage):
        repo.set_stage_running(conn, stage)

        if stage == "normalize":
            _run_normalize(conn)

        elif stage == "verify":
            _run_verify(conn)

        elif stage == "transfer_match":
            _run_transfer_match(conn)

        elif stage == "boil":
            _run_acb(conn, tax_rules)

        elif stage == "superficial_loss":
            _run_superficial_loss(conn, tax_rules)

        repo.set_stage_done(conn, stage)


def _run_normalize(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("Checking sap levels...")
    raw_txs = repo.read_raw_transactions(conn)
    if not raw_txs:
        raise _BoilError(MessageCode.BOIL_ERROR_NO_RAW_TRANSACTIONS)

    boc_attributed = _prefetch_boc_rates(conn, raw_txs)
    _prefetch_crypto_prices_bulk(conn, raw_txs, boc_already_attributed=boc_attributed)

    logger.info("processing %d raw transaction(s)", len(raw_txs))
    txs = normalizer.normalize(raw_txs, conn)
    txs = repo.write_transactions(conn, txs)
    logger.info("wrote %d normalized transaction(s)", len(txs))

    zero_count = sum(
        1
        for tx in txs
        if tx.cad_value == 0
        and tx.tx_type.value
        not in {
            "deposit",
            "withdrawal",
            "transfer_in",
            "transfer_out",
            "fiat_deposit",
            "fiat_withdrawal",
        }
    )
    if zero_count:
        emit(MessageCode.BOIL_NORMALIZE_ZERO_CAD_WARNING, count=zero_count)


def _prefetch_boc_rates(conn: object, raw_txs: list[RawTransaction]) -> bool:
    """Prefetch Bank of Canada USDCAD rates for the full date span of all transactions.

    Makes a single range HTTP call covering the entire batch date range instead
    of one-per-transaction, then caches all results so the normalizer and the
    crypto price converter never touch the network for BoC rates.

    The full range is always prefetched (not just USD-fiat dates) because
    get_crypto_price_cad() also calls get_rate() internally to convert USD
    crypto prices to CAD — even for CAD-only batches.

    Returns True if the BoC attribution message was emitted (so the caller can
    avoid a duplicate attribution if crypto prices also use BoC).
    """
    assert isinstance(conn, sqlite3.Connection)

    if not raw_txs:
        return False

    min_date = min(raw.timestamp.date() for raw in raw_txs)
    max_date = max(raw.timestamp.date() for raw in raw_txs)
    day_count = (max_date - min_date).days + 1
    emit(MessageCode.BOIL_NORMALIZE_PREFETCH_BOC, count=day_count)
    try:
        prefetch_rates(conn, "USDCAD", min_date, max_date)
        emit(MessageCode.BOIL_NORMALIZE_BOC_ATTRIBUTION)
        return True
    except BoCRateError as exc:
        logger.warning("BoC prefetch failed — %s. Will retry per-transaction.", exc)
        return False


def _make_fetch_progress(total: int) -> Callable[[int, int], None] | None:
    """Return a progress callback for the per-date crypto price fallback loop.

    On TTY: overwrites a single line in-place using carriage return so the
    display does not scroll.
    Off TTY (CI, pipes): prints a milestone line every ~10 % and at completion.
    Returns None when total == 0 (nothing to fetch).
    """
    if total == 0:
        return None
    is_tty = sys.stdout.isatty()
    width = len(str(total))
    milestone = max(1, total // 10)

    def _cb(done: int, _total: int) -> None:
        if is_tty:
            print(f"\r  [{done:{width}}/{_total}]", end="", flush=True)
        elif done % milestone == 0 or done == _total:
            print(f"  [{done}/{_total}]", flush=True)

    return _cb


def _prefetch_crypto_prices_bulk(
    conn: object,
    raw_txs: list[RawTransaction],
    *,
    boc_already_attributed: bool = False,
) -> None:
    """Prefetch crypto CAD prices for all no-fiat transactions.

    Strategy (two passes):
    1. Range pass — one CoinGecko ``/market_chart/range`` call per unique
       asset covers the full date span.  O(N_assets) HTTP calls instead of
       O(N_dates x N_assets).
    2. Per-date fallback — handles any dates the range response did not cover
       (CoinGecko gaps, very recent dates, unsupported assets).  Mempool.space
       is preferred for BTC in this pass.

    Results are cached in SQLite so the normalizer never hits the network for
    these prices.

    Pass boc_already_attributed=True when _prefetch_boc_rates already emitted
    the BoC attribution line, to avoid printing it twice in the same run.
    """
    assert isinstance(conn, sqlite3.Connection)

    pairs: list[tuple[str, date]] = [
        (raw.asset.upper(), raw.timestamp.date())
        for raw in raw_txs
        if raw.fiat_value is None or raw.fiat_currency is None
    ]
    # Deduplicate while preserving deterministic order for logging.
    seen_dedup: set[tuple[str, date]] = set()
    unique_pairs: list[tuple[str, date]] = []
    for p in pairs:
        if p not in seen_dedup:
            seen_dedup.add(p)
            unique_pairs.append(p)

    if not unique_pairs:
        return

    emit(MessageCode.BOIL_NORMALIZE_PREFETCH_CRYPTO, count=len(unique_pairs))

    # Pass 1: range-based bulk fetch (one CoinGecko call per unique asset).
    asset_date_groups: dict[str, list[date]] = {}
    for asset, d in unique_pairs:
        asset_date_groups.setdefault(asset, []).append(d)
    _range_written, range_cg = prefetch_crypto_prices_by_range(conn, asset_date_groups)

    # Pass 2: per-date fallback for any gaps left by the range call.
    progress_cb = _make_fetch_progress(len(unique_pairs))
    _per_total, per_cg, mempool_count = prefetch_crypto_prices(
        conn, unique_pairs, progress_cb=progress_cb
    )
    if sys.stdout.isatty() and progress_cb is not None:
        print()  # end the \r progress line before attribution messages

    coingecko_count = range_cg + per_cg
    if coingecko_count > 0:
        emit(MessageCode.BOIL_NORMALIZE_COINGECKO_ATTRIBUTION)
    if mempool_count > 0:
        emit(MessageCode.BOIL_NORMALIZE_MEMPOOL_ATTRIBUTION)
    if (coingecko_count > 0 or mempool_count > 0) and not boc_already_attributed:
        emit(MessageCode.BOIL_NORMALIZE_BOC_ATTRIBUTION)


def _run_verify(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("promoting transactions to verified (pass-through — no node)")
    count = repo.promote_to_verified(conn)
    logger.info("%d row(s) promoted to verified_transactions", count)


def _run_transfer_match(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("Tracing the flow...")
    txs = repo.read_transactions(conn)
    logger.info("classifying %d transaction(s)", len(txs))

    overrides = repo.read_transfer_overrides(conn)
    if overrides:
        logger.info(
            "applying %d stir override(s) (%d link, %d unlink)",
            len(overrides),
            sum(1 for o in overrides if o.action == "link"),
            sum(1 for o in overrides if o.action == "unlink"),
        )

    events, income_evts = matcher.match_transfers(txs, overrides=overrides)

    # The matcher uses transactions.id as vtx_id; patch to verified_transactions.id
    # before writing, since classified_events.vtx_id and income_events.vtx_id both
    # FK-reference verified_transactions.id (a separate autoincrement sequence).
    # On an untouched batch these IDs coincidentally match, but after a --from re-run
    # the sequences diverge and the FK insert fails without this correction.
    vtx_id_map = repo.read_verified_tx_id_map(conn)
    events = [dataclasses.replace(e, vtx_id=vtx_id_map.get(e.vtx_id, e.vtx_id)) for e in events]
    income_evts = [
        dataclasses.replace(e, vtx_id=vtx_id_map.get(e.vtx_id, e.vtx_id)) for e in income_evts
    ]

    events = repo.write_classified_events(conn, events)
    income_evts = repo.write_income_events(conn, income_evts)
    logger.info(
        "wrote %d classified event(s), %d income event(s)",
        len(events),
        len(income_evts),
    )


def _run_acb(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("Boiling the sap...")
    events = repo.read_classified_events(conn)
    logger.info("running ACB engine on %d taxable event(s)", len(events))

    disps, states = acb_engine.run(events, tax_rules)
    disps = repo.write_dispositions(conn, disps, states)
    logger.info("wrote %d disposition(s)", len(disps))


def _run_superficial_loss(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("scanning for 61-day window violations")
    disps = repo.read_dispositions(conn)
    all_events = repo.read_all_classified_events(conn)

    adjs = sld_engine.run(disps, all_events, tax_rules)
    adjs = repo.write_adjusted_dispositions(conn, adjs)
    logger.info("wrote %d adjusted disposition(s)", len(adjs))


def _load_tax_rules() -> TaxRules:
    """Load ``config/tax_rules.yaml`` and return a ``TaxRules`` dataclass."""
    if not _TAX_RULES_PATH.exists():
        raise _BoilError(MessageCode.BOIL_ERROR_NO_TAX_RULES, path=_TAX_RULES_PATH)

    with _TAX_RULES_PATH.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    return TaxRules(
        capital_gains_inclusion_rate=Decimal(str(raw["capital_gains_inclusion_rate"])),
        superficial_loss_window_days=int(raw["superficial_loss_window_days"]),
    )


def _should_run(conn: object, stage: str, from_stage: str | None) -> bool:
    """Return True if *stage* should execute in this run.

    Logic:
    - If ``--from`` was given, run all stages from that stage onward.
    - Otherwise, run only stages that are not ``done``.
    """
    assert isinstance(conn, sqlite3.Connection)

    status = repo.get_stage_status(conn, stage)

    if from_stage is not None:
        # Re-run everything from from_stage onward.
        from_idx = _BOIL_STAGES.index(from_stage)
        stage_idx = _BOIL_STAGES.index(stage)
        return stage_idx >= from_idx

    # No --from: skip completed stages, run pending/invalidated.
    return status != "done"


def _check_not_running(conn: object, stage: str, batch_name: str) -> None:
    """Raise _BoilError if *stage* is currently marked as running."""
    assert isinstance(conn, sqlite3.Connection)

    status = repo.get_stage_status(conn, stage)
    if status == "running":
        raise _BoilError(MessageCode.BOIL_ERROR_STAGE_RUNNING, name=batch_name, stage=stage)


def _validate_from_stage(from_stage: str) -> None:
    """Raise _BoilError if *from_stage* is not a valid boil stage."""
    if from_stage not in _BOIL_STAGES:
        raise _BoilError(
            MessageCode.BOIL_ERROR_UNKNOWN_STAGE,
            stage=from_stage,
            valid=", ".join(_BOIL_STAGES),
        )


def _stages_from(from_stage: str) -> tuple[str, ...]:
    """Return the subset of _BOIL_STAGES starting from *from_stage*."""
    idx = _BOIL_STAGES.index(from_stage)
    return _BOIL_STAGES[idx:]


def _print_summary(conn: object, batch_name: str) -> None:
    """Print a summary of row counts written to the batch."""
    assert isinstance(conn, sqlite3.Connection)

    def _count(table: str) -> int:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
        return int(row[0]) if row else 0

    emit(MessageCode.BOIL_SUMMARY_COMPLETE, name=batch_name)
    print(f"  transactions:          {_count('transactions'):>6}")
    print(f"  classified_events:     {_count('classified_events'):>6}")
    print(f"  income_events:         {_count('income_events'):>6}")
    print(f"  dispositions:          {_count('dispositions'):>6}")
    print(f"  dispositions_adjusted: {_count('dispositions_adjusted'):>6}")

    # Realised gain/loss.
    row = conn.execute(
        "SELECT SUM(CAST(adjusted_gain_loss AS REAL)) FROM dispositions_adjusted"
    ).fetchone()
    if row and row[0] is not None:
        net = row[0]
        sign = "+" if net >= 0 else ""
        print(f"\n  Realised gain/loss:    {sign}{net:,.2f} CAD (before inclusion rate)")

    # Year-end holdings — cost basis for open positions.
    holdings = conn.execute(
        """
        SELECT asset, pool_cost, units
        FROM acb_state
        WHERE id IN (SELECT MAX(id) FROM acb_state GROUP BY asset)
          AND CAST(units AS REAL) > 0
        ORDER BY asset
        """
    ).fetchall()
    if holdings:
        emit(MessageCode.BOIL_SUMMARY_HOLDINGS_HEADER)
        for h in holdings:
            units_val = float(h["units"])
            cost_val = float(h["pool_cost"])
            per_unit = cost_val / units_val if units_val else 0.0
            print(
                f"    {h['asset']:<6} {units_val:>14.8f} units"
                f"   ACB: {cost_val:>12,.2f} CAD"
                f"   ({per_unit:>12,.2f} CAD/unit)"
            )

    # Superficial losses — show details if any were found.
    sld_rows = conn.execute(
        """
        SELECT asset, timestamp, gain_loss, superficial_loss_denied, allowable_loss
        FROM dispositions_adjusted
        WHERE is_superficial_loss = 1
        ORDER BY timestamp
        """
    ).fetchall()
    if sld_rows:
        hr = "─" * 68
        print(f"\n  Superficial losses adjusted ({len(sld_rows)}):")
        print(f"  {hr}")
        for r in sld_rows:
            date_str = r["timestamp"][:10]
            denied = float(r["superficial_loss_denied"])
            allow = float(r["allowable_loss"])
            loss = float(r["gain_loss"])
            print(
                f"    {r['asset']:<5}  {date_str}"
                f"  loss: {loss:>10,.2f} CAD"
                f"  denied: {denied:>8,.2f}  allowable: {allow:>18.8f} CAD"
            )

    # Pipeline stage statuses.
    print()
    for stage in PIPELINE_STAGES:
        row = conn.execute(
            "SELECT status, completed_at FROM stage_status WHERE stage = ?", (stage,)
        ).fetchone()
        if row:
            status = row["status"]
            completed = row["completed_at"] or ""
            print(f"  [{status:>11}]  {stage}  {completed[:10]}")
