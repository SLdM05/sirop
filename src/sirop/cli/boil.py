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

import csv
import dataclasses
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.db.schema import PIPELINE_STAGES
from sirop.engine import acb as acb_engine
from sirop.engine import superficial_loss as sld_engine
from sirop.engine.acb import TaxRules
from sirop.models.messages import MessageCode
from sirop.node.privacy import is_private_node_url
from sirop.normalizer import normalizer
from sirop.transfer_match import matcher
from sirop.utils.boc import BoCRateError, fill_rate_gaps, prefetch_rates
from sirop.utils.crypto_prices import prefetch_crypto_prices
from sirop.utils.logging import StageContext, get_logger
from sirop.utils.messages import emit, spinner
from sirop.utils.price_cache import copy_prices_into_batch, open_price_cache, sync_prices_to_cache

if TYPE_CHECKING:
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
    audit: bool = False,
    settings: Settings | None = None,
    allow_public_mempool: bool = False,
) -> int:
    """Run the pipeline from *from_stage* (or from the beginning if None).

    Parameters
    ----------
    from_stage:
        When provided, all stages before this one are skipped even if they
        are pending.  Stages after it (and itself) are re-run even if done.
    audit:
        When True, write an ACB ledger CSV after the pipeline completes.
    settings:
        Application settings; resolved from the environment if omitted.
    allow_public_mempool:
        When True, skip the interactive privacy prompt even if
        ``BTC_MEMPOOL_URL`` points to a public host.  Equivalent to setting
        ``BTC_TRAVERSAL_ALLOW_PUBLIC=true`` in the environment.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on any error.
    """
    if settings is None:
        settings = get_settings()
    try:
        return _run_boil(from_stage, audit, settings, allow_public_mempool=allow_public_mempool)
    except _BoilError as exc:
        emit(exc.msg_code, **exc.msg_kwargs)
        return 1


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run_boil(
    from_stage: str | None,
    audit: bool,
    settings: Settings,
    *,
    allow_public_mempool: bool = False,
) -> int:
    """Core pipeline coordinator.  Raises ``_BoilError`` on any user-facing error."""
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _BoilError(MessageCode.BATCH_ERROR_NO_ACTIVE)

    conn = open_batch(batch_name, settings)
    cache_conn: sqlite3.Connection | None = None
    if settings.asset_price_cache:
        cache_conn = open_price_cache(settings.data_dir)
    try:
        # Ensure tap has been run.
        tap_status = repo.get_stage_status(conn, "tap")
        if tap_status != "done":
            raise _BoilError(MessageCode.BOIL_ERROR_NOT_TAPPED, name=batch_name)

        tax_rules = _load_tax_rules()

        # Resolve graph traversal permission once before any stage runs.
        graph_traversal_allowed = _resolve_graph_traversal_permission(
            settings, allow_public_mempool=allow_public_mempool
        )

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
            _execute_stage(
                conn,
                stage,
                batch_name,
                tax_rules,
                cache_conn=cache_conn,
                graph_traversal_allowed=graph_traversal_allowed,
            )

        _print_summary(conn, batch_name)
        if audit:
            _run_audit(conn, batch_name, settings)
        return 0

    finally:
        conn.close()
        if cache_conn is not None:
            cache_conn.close()


def _execute_stage(  # noqa: PLR0913
    conn: object,  # sqlite3.Connection — typed generically to avoid circular import hint
    stage: str,
    batch_name: str,
    tax_rules: TaxRules,
    cache_conn: sqlite3.Connection | None = None,
    graph_traversal_allowed: bool = True,
) -> None:
    """Execute a single pipeline stage wrapped in StageContext."""
    assert isinstance(conn, sqlite3.Connection)

    with StageContext(batch_id=batch_name, stage=stage):
        repo.set_stage_running(conn, stage)

        if stage == "normalize":
            _run_normalize(conn, cache_conn=cache_conn)

        elif stage == "verify":
            _run_verify(conn)

        elif stage == "transfer_match":
            _run_transfer_match(conn, graph_traversal_allowed=graph_traversal_allowed)

        elif stage == "boil":
            _run_acb(conn, tax_rules)

        elif stage == "superficial_loss":
            _run_superficial_loss(conn, tax_rules)

        repo.set_stage_done(conn, stage)


def _run_normalize(
    conn: object,
    *,
    cache_conn: sqlite3.Connection | None = None,
) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.debug("Checking sap levels...")
    raw_txs = repo.read_raw_transactions(conn)
    if not raw_txs:
        raise _BoilError(MessageCode.BOIL_ERROR_NO_RAW_TRANSACTIONS)

    if cache_conn is not None:
        min_date = min(r.timestamp.date() for r in raw_txs)
        max_date = max(r.timestamp.date() for r in raw_txs)
        boc_hit, crypto_hit = copy_prices_into_batch(cache_conn, conn, min_date, max_date)
        logger.debug(
            "price cache: pre-loaded %d BoC rate(s), %d crypto price(s)",
            boc_hit,
            crypto_hit,
        )

    boc_attributed = _prefetch_boc_rates(conn, raw_txs)
    _prefetch_crypto_prices_bulk(conn, raw_txs, boc_already_attributed=boc_attributed)

    logger.debug("processing %d raw transaction(s)", len(raw_txs))
    with spinner("Normalizing transactions…"):
        txs = normalizer.normalize(raw_txs, conn)
    txs = repo.write_transactions(conn, txs)
    logger.debug("wrote %d normalized transaction(s)", len(txs))

    if cache_conn is not None:
        boc_synced, crypto_synced = sync_prices_to_cache(conn, cache_conn)
        logger.debug(
            "price cache: synced %d BoC rate(s), %d crypto price(s)",
            boc_synced,
            crypto_synced,
        )

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
        with spinner("Fetching Bank of Canada rates…"):
            prefetch_rates(conn, "USDCAD", min_date, max_date)
        fill_rate_gaps(conn, "USDCAD", min_date, max_date)
        emit(MessageCode.BOIL_NORMALIZE_BOC_ATTRIBUTION)
        return True
    except BoCRateError as exc:
        logger.warning("BoC prefetch failed — %s. Will retry per-transaction.", exc)
        return False


def _prefetch_crypto_prices_bulk(
    conn: object,
    raw_txs: list[RawTransaction],
    *,
    boc_already_attributed: bool = False,
) -> None:
    """Prefetch BTC CAD prices for all no-fiat transactions via Mempool.space.

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

    total = len(unique_pairs)
    with spinner(f"Fetching crypto prices… [0/{total}]") as status:

        def _progress(done: int, _total: int) -> None:
            status.update(f"Fetching crypto prices… [{done}/{_total}]")

        _fetched, mempool_count = prefetch_crypto_prices(conn, unique_pairs, progress_cb=_progress)

    if mempool_count > 0:
        emit(MessageCode.BOIL_NORMALIZE_MEMPOOL_ATTRIBUTION)
    if mempool_count > 0 and not boc_already_attributed:
        emit(MessageCode.BOIL_NORMALIZE_BOC_ATTRIBUTION)


def _run_verify(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.debug("promoting transactions to verified (pass-through — no node)")
    count = repo.promote_to_verified(conn)
    logger.debug("%d row(s) promoted to verified_transactions", count)


def _resolve_graph_traversal_permission(
    settings: Settings, *, allow_public_mempool: bool = False
) -> bool:
    """Return True when graph traversal is permitted for the configured Mempool URL.

    Rules (evaluated in order):
    1. If ``BTC_TRAVERSAL_MAX_HOPS == 0`` — traversal disabled, return True
       (the hop-gate in matcher.py will skip Pass 1b anyway; no prompt needed).
    2. If the URL is a private/local address — return True silently.
    3. If ``allow_public_mempool`` (CLI flag) or
       ``settings.btc_traversal_allow_public`` (env var) — emit warning then
       return True (non-interactive confirmation already given).
    4. Otherwise — prompt the user interactively.  Return True on "y/Y", False
       on anything else (including EOF / non-interactive stdin).
    """
    if settings.btc_traversal_max_hops <= 0:
        return True

    url = settings.btc_mempool_url
    if is_private_node_url(url):
        return True

    # Public URL — check for non-interactive bypass first.
    if allow_public_mempool or settings.btc_traversal_allow_public:
        emit(MessageCode.BOIL_GRAPH_PRIVACY_WARNING, url=url)
        return True

    # Interactive prompt.
    emit(MessageCode.BOIL_GRAPH_PRIVACY_WARNING, url=url)
    try:
        answer = input("Proceed with graph traversal? [y/N] ").strip().lower()
    except (EOFError, OSError):
        answer = ""

    if answer == "y":
        return True

    emit(MessageCode.BOIL_GRAPH_PRIVACY_SKIPPED)
    return False


def _run_transfer_match(conn: object, *, graph_traversal_allowed: bool = True) -> None:
    assert isinstance(conn, sqlite3.Connection)

    tax_year = repo.read_tax_year(conn)
    logger.debug("Tracing the flow...")
    txs = repo.read_transactions(conn)
    logger.debug("classifying %d transaction(s)", len(txs))

    overrides = repo.read_transfer_overrides(conn)
    if overrides:
        logger.info(
            "applying %d stir override(s) (%d link, %d unlink)",
            len(overrides),
            sum(1 for o in overrides if o.action == "link"),
            sum(1 for o in overrides if o.action == "unlink"),
        )

    # Apply user-supplied txid overrides (from `sirop stir destination`) before
    # matching.  These patch in blockchain txids for transactions whose CSV did
    # not include one (e.g. NDAX withdrawals), making them eligible for Pass 1
    # exact matching and Pass 1b graph traversal.
    txid_overrides = repo.read_transaction_txid_overrides(conn)
    if txid_overrides:
        logger.info("applying %d user-supplied txid override(s)", len(txid_overrides))
        txs = [
            dataclasses.replace(t, txid=txid_overrides[t.id])
            if t.id in txid_overrides and t.txid is None
            else t
            for t in txs
        ]

    with spinner("Classifying events…") as _status:

        def _graph_progress(api_calls: int, done: int, total: int, found: int) -> None:
            noun = "match" if found == 1 else "matches"
            _status.update(
                f"Traversing UTXO graph… "
                f"[{done}/{total} · {api_calls} API calls · {found} {noun} found]"
            )

        events, income_evts, graph_matches = matcher.match_transfers(
            txs,
            overrides=overrides,
            tax_year=tax_year,
            graph_traversal_allowed=graph_traversal_allowed,
            on_graph_progress=_graph_progress,
        )
    repo.write_graph_transfer_pairs(conn, graph_matches)

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
    logger.debug(
        "wrote %d classified event(s), %d income event(s)",
        len(events),
        len(income_evts),
    )

    # Emit a single consolidated warning if future-year disposals were found.
    # Per-event W002 is suppressed by the matcher for future-year events.
    future_sell_count = sum(
        1 for e in events if e.event_type == "sell" and e.timestamp.year > tax_year
    )
    if future_sell_count:
        emit(
            MessageCode.BOIL_WARNING_FUTURE_YEAR_DISPOSITIONS,
            count=future_sell_count,
            tax_year=tax_year,
        )


def _run_acb(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    tax_year = repo.read_tax_year(conn)
    all_events = repo.read_classified_events(conn)
    # Feed only current-year events to the ACB engine.
    # Future-year acquisitions remain in classified_events for SL detection.
    events = [e for e in all_events if e.timestamp.year <= tax_year]
    logger.debug(
        "running ACB engine on %d taxable event(s) (%d total, %d future-year excluded)",
        len(events),
        len(all_events),
        len(all_events) - len(events),
    )

    with spinner("Running ACB engine…"):
        disps, states, final_pools, last_events, underruns = acb_engine.run(events, tax_rules)

    for u in underruns:
        emit(
            MessageCode.BOIL_ACB_POOL_UNDERRUN,
            asset=u.asset,
            date=u.timestamp.date(),
            attempted=format(u.attempted, "f"),
            available=format(u.available, "f"),
        )

    disps = repo.write_dispositions(conn, disps, states)
    logger.debug("wrote %d disposition(s)", len(disps))

    # Write year-end snapshots for assets acquired but never sold in this tax year.
    # Without these, the holdings query finds nothing for those assets.
    assets_with_disposal = {s.asset for s in states}
    holdovers = [
        (pool, last_events[asset].id)
        for asset, pool in final_pools.items()
        if asset not in assets_with_disposal and pool.total_units > Decimal("0")
    ]
    if holdovers:
        repo.write_holdover_acb_states(conn, holdovers)
        logger.debug("wrote %d holdover acb_state snapshot(s)", len(holdovers))


def _run_superficial_loss(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.debug("scanning for 61-day window violations")
    disps = repo.read_dispositions(conn)
    all_events = repo.read_all_classified_events(conn)

    with spinner("Detecting superficial losses…"):
        adjs = sld_engine.run(disps, all_events, tax_rules)
    adjs = repo.write_adjusted_dispositions(conn, adjs)
    logger.debug("wrote %d adjusted disposition(s)", len(adjs))


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


_AUDIT_COLUMNS = [
    "Date",
    "Time (UTC)",
    "Asset",
    "Event Type",
    "Source",
    "Units",
    "CAD Cost (ACQ)",
    "Fees (CAD)",
    "Pool Units Before",
    "Pool Cost Before (CAD)",
    "ACB/Unit Before (CAD)",
    "Proceeds (CAD)",
    "ACB of Disposed (CAD)",
    "Selling Fees (CAD)",
    "Gross Gain/Loss (CAD)",
    "Superficial Loss",
    "Denied Loss (CAD)",
    "Allowable Loss (CAD)",
    "Net Gain/Loss (CAD)",
    "Pool Units After",
    "Pool Cost After (CAD)",
    "ACB/Unit After (CAD)",
    "Year Acquired",
]

_ROUNDING = Decimal("0.00000001")


def _run_audit(conn: sqlite3.Connection, batch_name: str, settings: Settings) -> None:
    """Write a chronological ACB ledger CSV for manual verification.

    One row per classified taxable event (acquisitions and disposals).
    Acquisition rows show the running pool evolution computed by replaying
    the same weighted-average math as the ACB engine.  Disposal rows pull
    authoritative values from the database (``dispositions_adjusted``).
    """
    if repo.get_stage_status(conn, "superficial_loss") != "done":
        raise _BoilError(MessageCode.BOIL_AUDIT_ERROR_NOT_READY)

    # Load all taxable classified events sorted chronologically.
    all_events = repo.read_classified_events(conn)
    taxable = sorted(
        (e for e in all_events if e.is_taxable),
        key=lambda e: (e.timestamp, e.id),
    )

    # Pre-load disposal details keyed by classified_event id.
    disposal_rows = conn.execute(
        """
        SELECT
            d.event_id,
            d.acb_per_unit_before,
            d.pool_units_before,
            d.pool_cost_before,
            d.acb_per_unit_after,
            d.pool_units_after,
            d.pool_cost_after,
            da.proceeds,
            da.acb_of_disposed,
            da.selling_fees,
            da.gain_loss,
            da.is_superficial_loss,
            da.superficial_loss_denied,
            da.allowable_loss,
            da.adjusted_gain_loss,
            da.year_acquired,
            da.disposition_type
        FROM dispositions_adjusted da
        JOIN dispositions d ON da.disposition_id = d.id
        """
    ).fetchall()
    disposal_map: dict[int, Any] = {r["event_id"]: r for r in disposal_rows}

    # Per-asset running pool (total_units, total_acb_cad) — used for acquisition rows.
    # Disposal after-states are taken from the DB and used to keep the running pool
    # consistent for any acquisitions that follow a disposal.
    pools: dict[str, tuple[Decimal, Decimal]] = {}

    out_rows: list[dict[str, object]] = []

    for event in taxable:
        asset = event.asset
        pool_units, pool_cost = pools.get(asset, (Decimal("0"), Decimal("0")))

        if event.event_type in ("buy", "income", "other"):
            acb_per_unit_before = (
                (pool_cost / pool_units).quantize(_ROUNDING) if pool_units else Decimal("0")
            )

            cad_cost = event.cad_cost or Decimal("0")
            cad_fee = event.cad_fee or Decimal("0")
            pool_units_after = pool_units + event.amount
            pool_cost_after = pool_cost + cad_cost + cad_fee
            acb_per_unit_after = (
                (pool_cost_after / pool_units_after).quantize(_ROUNDING)
                if pool_units_after
                else Decimal("0")
            )
            pools[asset] = (pool_units_after, pool_cost_after)

            out_rows.append(
                {
                    "Date": event.timestamp.date().isoformat(),
                    "Time (UTC)": event.timestamp.time().isoformat(timespec="seconds"),
                    "Asset": asset,
                    "Event Type": "Acquisition",
                    "Source": event.source,
                    "Units": format(event.amount, "f"),
                    "CAD Cost (ACQ)": format(cad_cost, "f"),
                    "Fees (CAD)": format(cad_fee, "f"),
                    "Pool Units Before": format(pool_units, "f"),
                    "Pool Cost Before (CAD)": format(pool_cost, "f"),
                    "ACB/Unit Before (CAD)": format(acb_per_unit_before, "f"),
                    "Proceeds (CAD)": "",
                    "ACB of Disposed (CAD)": "",
                    "Selling Fees (CAD)": "",
                    "Gross Gain/Loss (CAD)": "",
                    "Superficial Loss": "",
                    "Denied Loss (CAD)": "",
                    "Allowable Loss (CAD)": "",
                    "Net Gain/Loss (CAD)": "",
                    "Pool Units After": format(pool_units_after, "f"),
                    "Pool Cost After (CAD)": format(pool_cost_after, "f"),
                    "ACB/Unit After (CAD)": format(acb_per_unit_after, "f"),
                    "Year Acquired": "",
                }
            )

        elif event.event_type in ("sell", "fee_disposal", "spend"):
            d = disposal_map.get(event.id)
            if d is None:
                logger.warning("audit: no disposal row found for event_id=%d, skipping", event.id)
                continue

            # Update running pool from DB's authoritative after-state so subsequent
            # acquisition rows see the correct pool balance.
            pools[asset] = (Decimal(d["pool_units_after"]), Decimal(d["pool_cost_after"]))

            is_sl = bool(d["is_superficial_loss"])
            out_rows.append(
                {
                    "Date": event.timestamp.date().isoformat(),
                    "Time (UTC)": event.timestamp.time().isoformat(timespec="seconds"),
                    "Asset": asset,
                    "Event Type": "Disposal",
                    "Source": event.source,
                    "Units": format(
                        Decimal(d["pool_units_before"]) - Decimal(d["pool_units_after"]), "f"
                    ),
                    "CAD Cost (ACQ)": "",
                    "Fees (CAD)": "",
                    "Pool Units Before": d["pool_units_before"],
                    "Pool Cost Before (CAD)": d["pool_cost_before"],
                    "ACB/Unit Before (CAD)": d["acb_per_unit_before"],
                    "Proceeds (CAD)": d["proceeds"],
                    "ACB of Disposed (CAD)": d["acb_of_disposed"],
                    "Selling Fees (CAD)": d["selling_fees"],
                    "Gross Gain/Loss (CAD)": d["gain_loss"],
                    "Superficial Loss": "Yes" if is_sl else "No",
                    "Denied Loss (CAD)": d["superficial_loss_denied"],
                    "Allowable Loss (CAD)": d["allowable_loss"],
                    "Net Gain/Loss (CAD)": d["adjusted_gain_loss"],
                    "Pool Units After": d["pool_units_after"],
                    "Pool Cost After (CAD)": d["pool_cost_after"],
                    "ACB/Unit After (CAD)": d["acb_per_unit_after"],
                    "Year Acquired": d["year_acquired"],
                }
            )

    audit_dir = settings.output_dir / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    out_path = audit_dir / f"{batch_name}-audit.csv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)

    emit(MessageCode.BOIL_AUDIT_WRITTEN, path=out_path, count=len(out_rows))


def _print_income_and_costs(conn: sqlite3.Connection) -> None:
    """Print income and costs & expenses sections of the boil summary."""
    income_rows = conn.execute(
        """
        SELECT income_type, SUM(CAST(fmv_cad AS REAL)) AS total
        FROM income_events
        GROUP BY income_type
        ORDER BY income_type
        """
    ).fetchall()
    if income_rows:
        total_income = sum(float(r["total"]) for r in income_rows)
        print(f"  Income:                {total_income:>12,.2f} CAD")
        for r in income_rows:
            print(f"    {r['income_type']:<20} {float(r['total']):>10,.2f} CAD")

    costs_row = conn.execute(
        "SELECT COALESCE(SUM(CAST(cad_fee AS REAL)), 0) "
        "FROM classified_events "
        "WHERE is_taxable = 1 AND cad_fee IS NOT NULL"
    ).fetchone()
    if costs_row and costs_row[0]:
        print(f"  Costs & expenses:      {float(costs_row[0]):>12,.2f} CAD")


def _print_wallet_holdings(
    conn: sqlite3.Connection, per_unit_acb: dict[str, float], tax_year: int
) -> None:
    """Print year-end holdings broken down by wallet."""
    year_end = f"{tax_year}-12-31"
    wallet_rows = conn.execute(
        """
        WITH transfer_vtx_ids AS (
            -- vtx_ids of matched transfer legs; their fee is already captured
            -- in the amount difference (W - D) and must not be double-subtracted.
            SELECT DISTINCT vtx_id FROM classified_events WHERE event_type = 'transfer'
        ),
        fee_disposal_adj AS (
            -- Sum fee_disposal amounts per wallet/asset for non-transfer transactions
            -- within the tax year.  These crypto fees reduce the global ACB pool but
            -- are not reflected in transactions.amount, so per-wallet holdings would
            -- otherwise overstate.
            SELECT
                ce.wallet_id,
                ce.asset,
                SUM(CAST(ce.amount AS REAL)) AS adj
            FROM classified_events ce
            WHERE ce.event_type = 'fee_disposal'
              AND ce.is_taxable = 1
              AND ce.timestamp <= ?
              AND ce.vtx_id NOT IN (SELECT vtx_id FROM transfer_vtx_ids)
            GROUP BY ce.wallet_id, ce.asset
        )
        SELECT
            COALESCE(w.name, t.source) AS wallet_name,
            t.asset,
            SUM(CASE
                WHEN t.transaction_type IN ('buy','transfer_in','deposit','income')
                    THEN CAST(t.amount AS REAL)
                WHEN t.transaction_type IN ('sell','transfer_out','withdrawal','spend','fee')
                    THEN -CAST(t.amount AS REAL)
                ELSE 0
            END) - COALESCE(MAX(fda.adj), 0) AS net_units
        FROM transactions t
        LEFT JOIN wallets w ON t.wallet_id = w.id
        LEFT JOIN fee_disposal_adj fda ON fda.wallet_id = t.wallet_id AND fda.asset = t.asset
        WHERE t.timestamp <= ?
          AND t.asset IN (
            SELECT asset FROM acb_state
            WHERE id IN (SELECT MAX(id) FROM acb_state GROUP BY asset)
              AND CAST(units AS REAL) > 0
        )
        GROUP BY wallet_name, t.asset
        HAVING net_units > 0.00000001
        ORDER BY wallet_name, t.asset
        """,
        (year_end, year_end),
    ).fetchall()

    wallet_assets: dict[str, list[Any]] = {}
    for r in wallet_rows:
        wallet_assets.setdefault(r["wallet_name"], []).append(r)

    # External wallets — net units held in wallets flagged via `stir external`.
    # external-out: units left a tracked wallet → positive for the external wallet
    # external-in:  units returned to a tracked wallet → negative for the external wallet
    ext_rows = conn.execute(
        """
        SELECT
            tor.external_wallet AS wallet_name,
            t.asset,
            SUM(CASE
                WHEN tor.action = 'external-out' THEN  CAST(t.amount AS REAL)
                WHEN tor.action = 'external-in'  THEN -CAST(t.amount AS REAL)
                ELSE 0
            END) AS net_units
        FROM transfer_overrides tor
        JOIN transactions t ON tor.tx_id_a = t.id
        WHERE tor.action IN ('external-out', 'external-in')
          AND tor.external_wallet != ''
          AND t.asset IN (
              SELECT asset FROM acb_state
              WHERE id IN (SELECT MAX(id) FROM acb_state GROUP BY asset)
                AND CAST(units AS REAL) > 0
          )
        GROUP BY tor.external_wallet, t.asset
        HAVING net_units > 0.00000001
        ORDER BY tor.external_wallet, t.asset
        """
    ).fetchall()
    for r in ext_rows:
        wallet_assets.setdefault(r["wallet_name"], []).append(r)

    for wallet_name, rows in wallet_assets.items():
        emit(MessageCode.BOIL_SUMMARY_WALLET_HEADER, name=wallet_name)
        emit(MessageCode.BOIL_SUMMARY_HOLDINGS_HEADER)
        for r in rows:
            u = float(r["net_units"])
            acb_pu = per_unit_acb.get(r["asset"], 0.0)
            wallet_acb = u * acb_pu
            print(
                f"    {r['asset']:<6} {u:>14.8f} units"
                f"   ACB: {wallet_acb:>12,.2f} CAD"
                f"   ({acb_pu:>12,.2f} CAD/unit)"
            )


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

    _print_income_and_costs(conn)

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
        per_unit_acb: dict[str, float] = {}
        for h in holdings:
            units_val = float(h["units"])
            cost_val = float(h["pool_cost"])
            per_unit = cost_val / units_val if units_val else 0.0
            per_unit_acb[h["asset"]] = per_unit
            print(
                f"    {h['asset']:<6} {units_val:>14.8f} units"
                f"   ACB: {cost_val:>12,.2f} CAD"
                f"   ({per_unit:>12,.2f} CAD/unit)"
            )

        tax_year = repo.read_tax_year(conn)
        _print_wallet_holdings(conn, per_unit_acb, tax_year)

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

    # Consolidated stir hint — only shown when unmatched transfers exist.
    unmatched = conn.execute(
        """
        SELECT
            SUM(CASE WHEN vt.transaction_type = 'withdrawal' THEN 1 ELSE 0 END) AS w,
            SUM(CASE WHEN vt.transaction_type = 'deposit'    THEN 1 ELSE 0 END) AS d
        FROM classified_events ce
        JOIN verified_transactions vt ON ce.vtx_id = vt.id
        WHERE vt.transaction_type IN ('withdrawal', 'deposit')
          AND ce.event_type IN ('sell', 'buy')
        """
    ).fetchone()
    if unmatched and (unmatched["w"] or unmatched["d"]):
        print()
        emit(
            MessageCode.BOIL_SUMMARY_STIR_HINT,
            w=unmatched["w"] or 0,
            d=unmatched["d"] or 0,
        )
