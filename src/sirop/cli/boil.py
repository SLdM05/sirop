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

import sqlite3
from decimal import Decimal
from pathlib import Path

import yaml

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.db.schema import PIPELINE_STAGES
from sirop.engine import acb as acb_engine
from sirop.engine import superficial_loss as sld_engine
from sirop.engine.acb import TaxRules
from sirop.normalizer import normalizer
from sirop.transfer_match import matcher
from sirop.utils.logging import StageContext, get_logger

logger = get_logger(__name__)

# Stages this command is responsible for (subset of PIPELINE_STAGES).
_BOIL_STAGES = ("normalize", "verify", "transfer_match", "boil", "superficial_loss")

# Path to the built-in tax rules config relative to the working directory.
_TAX_RULES_PATH = Path("config/tax_rules.yaml")


class _BoilError(Exception):
    """Sentinel — raised to exit early with a user-facing error message."""


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
        print(str(exc))
        return 1


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run_boil(from_stage: str | None, settings: Settings) -> int:
    """Core pipeline coordinator.  Raises ``_BoilError`` on any user-facing error."""
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _BoilError("error: no active batch. Run `sirop create <name>` first.")

    conn = open_batch(batch_name, settings)
    try:
        # Ensure tap has been run.
        tap_status = repo.get_stage_status(conn, "tap")
        if tap_status != "done":
            raise _BoilError(
                f"error: batch '{batch_name}' has not been tapped yet. "
                "Run `sirop tap <file>` first."
            )

        tax_rules = _load_tax_rules()

        # Invalidate stages that will be re-run.
        if from_stage is not None:
            _validate_from_stage(from_stage)
            stages_to_invalidate = _stages_from(from_stage)
            repo.set_stages_invalidated(conn, list(stages_to_invalidate))

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
        raise _BoilError("error: no raw transactions found. Run `sirop tap` first.")

    logger.info("normalize: processing %d raw transaction(s)", len(raw_txs))
    txs = normalizer.normalize(raw_txs, conn)
    txs = repo.write_transactions(conn, txs)
    logger.info("normalize: wrote %d normalized transaction(s)", len(txs))


def _run_verify(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("verify: promoting transactions to verified (pass-through — no node)")
    count = repo.promote_to_verified(conn)
    logger.info("verify: %d row(s) promoted to verified_transactions", count)


def _run_transfer_match(conn: object) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("Tracing the flow...")
    txs = repo.read_transactions(conn)
    logger.info("transfer_match: classifying %d transaction(s)", len(txs))

    events, income_evts = matcher.match_transfers(txs)
    events = repo.write_classified_events(conn, events)
    income_evts = repo.write_income_events(conn, income_evts)
    logger.info(
        "transfer_match: wrote %d classified event(s), %d income event(s)",
        len(events),
        len(income_evts),
    )


def _run_acb(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("Boiling the sap...")
    events = repo.read_classified_events(conn)
    logger.info("boil: running ACB engine on %d taxable event(s)", len(events))

    disps, states = acb_engine.run(events, tax_rules)
    disps = repo.write_dispositions(conn, disps, states)
    logger.info("boil: wrote %d disposition(s)", len(disps))


def _run_superficial_loss(conn: object, tax_rules: TaxRules) -> None:
    assert isinstance(conn, sqlite3.Connection)

    logger.info("superficial_loss: scanning for 61-day window violations")
    disps = repo.read_dispositions(conn)
    all_events = repo.read_all_classified_events(conn)

    adjs = sld_engine.run(disps, all_events, tax_rules)
    adjs = repo.write_adjusted_dispositions(conn, adjs)
    logger.info("superficial_loss: wrote %d adjusted disposition(s)", len(adjs))


def _load_tax_rules() -> TaxRules:
    """Load ``config/tax_rules.yaml`` and return a ``TaxRules`` dataclass."""
    if not _TAX_RULES_PATH.exists():
        raise _BoilError(
            f"error: tax rules config not found at {_TAX_RULES_PATH}. "
            "Is the working directory the project root?"
        )

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
        raise _BoilError(
            f"error: batch '{batch_name}' stage '{stage}' is currently running "
            "(another process?). Aborting."
        )


def _validate_from_stage(from_stage: str) -> None:
    """Raise _BoilError if *from_stage* is not a valid boil stage."""
    if from_stage not in _BOIL_STAGES:
        valid = ", ".join(_BOIL_STAGES)
        raise _BoilError(f"error: unknown stage {from_stage!r}. Valid stages: {valid}")


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

    print(f"\nBatch '{batch_name}' pipeline complete.")
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
        print("\n  Year-end holdings (cost basis):")
        for h in holdings:
            units_val = float(h["units"])
            cost_val = float(h["pool_cost"])
            per_unit = cost_val / units_val if units_val else 0.0
            print(
                f"    {h['asset']:<6} {units_val:>14.8f} units"
                f"   ACB: {cost_val:>12,.2f} CAD"
                f"   ({per_unit:>12,.2f} CAD/unit)"
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
