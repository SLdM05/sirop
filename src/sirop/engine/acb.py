"""ACB engine — pipeline stage 5 (PURE).

Implements the CRA-mandated weighted-average Adjusted Cost Base (ACB)
method for Canadian crypto tax reporting.

This module is PURE: it receives typed dataclasses and returns typed
dataclasses.  It never imports from ``src/db/``, reads files, calls APIs,
or accesses config.  All dependencies are injected by the pipeline
coordinator (``cli/boil.py``).

References
----------
- CLAUDE.md §3 — ACB formulas
- docs/ref/crypto-tax-reference-quebec-2025.md §3 — worked examples
- CRA Interpretation Bulletin IT-479R — capital property treatment
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.models.disposition import ACBState, Disposition
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from sirop.models.event import ClassifiedEvent

logger = get_logger(__name__)

# Precision context for ACB divisions to avoid runaway decimals.
_ROUNDING = Decimal("0.00000001")  # 8 decimal places


class InsufficientUnitsError(Exception):
    """Raised when a disposal exceeds the current pool balance.

    This can occur if source data is incomplete (e.g. a sell was imported
    without the corresponding buy, or a transfer was incorrectly treated
    as a disposal).
    """


@dataclass(frozen=True)
class TaxRules:
    """Tax parameters loaded from ``config/tax_rules.yaml``."""

    capital_gains_inclusion_rate: Decimal
    superficial_loss_window_days: int


def run(
    events: list[ClassifiedEvent],
    tax_rules: TaxRules,
) -> tuple[list[Disposition], list[ACBState], dict[str, ACBState], dict[str, ClassifiedEvent]]:
    """Compute ACB and capital gains for all *events*.

    Parameters
    ----------
    events:
        Taxable classified events in chronological order (output of
        ``repositories.read_classified_events()``).  Must be sorted by
        ``timestamp`` ascending.
    tax_rules:
        Tax parameters for the batch year.

    Returns
    -------
    tuple[list[Disposition], list[ACBState], dict[str, ACBState], dict[str, ClassifiedEvent]]
        - First element: one ``Disposition`` per disposal event.
        - Second element: ACB pool snapshots — one per disposal event,
          in the same order.  The repository writes these to ``acb_state``.
        - Third element: final pool state per asset after all events.
        - Fourth element: last processed event per asset (used to record
          year-end holdover snapshots for assets with no disposals).

    Raises
    ------
    InsufficientUnitsError
        When a disposal tries to sell more units than the pool holds.
    """
    # Per-asset ACB pools.  Keyed by asset symbol (e.g. "BTC").
    pools: dict[str, ACBState] = {}

    # Track acquisition years per asset so we can report "Various" when needed.
    acquisition_years: dict[str, set[int]] = {}

    # Track the last event processed per asset for holdover snapshot event_id.
    last_event: dict[str, ClassifiedEvent] = {}

    dispositions: list[Disposition] = []
    acb_states: list[ACBState] = []

    for event in sorted(events, key=lambda e: e.timestamp):
        asset = event.asset

        if asset not in pools:
            pools[asset] = ACBState.zero(asset)
            acquisition_years[asset] = set()

        pool = pools[asset]

        if event.event_type in {"buy", "income"}:
            pool = _process_acquisition(event, pool, acquisition_years[asset])
            pools[asset] = pool
            last_event[asset] = event

        elif event.event_type in {"sell", "fee_disposal", "spend"}:
            disp, new_pool = _process_disposal(event, pool, acquisition_years[asset])
            pools[asset] = new_pool
            dispositions.append(disp)
            acb_states.append(new_pool)
            last_event[asset] = event

        else:
            # Non-taxable events (transfers, fiat, other) are silently skipped.
            logger.debug("acb: skipping event_type=%s for %s", event.event_type, asset)

    logger.info(
        "acb: processed %d events → %d dispositions across %d asset(s)",
        len(events),
        len(dispositions),
        len(pools),
    )
    return dispositions, acb_states, pools, last_event


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _process_acquisition(
    event: ClassifiedEvent,
    pool: ACBState,
    acq_years: set[int],
) -> ACBState:
    """Update *pool* for an acquisition event and return the new pool.

    On acquisition:
        new_total_ACB = previous_total_ACB + purchase_cost_CAD + purchase_fees_CAD
        new_total_units = previous_total_units + units_acquired
        ACB_per_unit = new_total_ACB / new_total_units
    """
    cost = event.cad_cost or Decimal("0")
    fee = event.cad_fee or Decimal("0")

    new_total_acb = pool.total_acb_cad + cost + fee
    new_units = pool.total_units + event.amount

    new_acb_per_unit = (
        (new_total_acb / new_units).quantize(_ROUNDING)
        if new_units > Decimal("0")
        else Decimal("0")
    )

    acq_years.add(event.timestamp.year)

    logger.debug(
        "acb: buy %s %s — cost=%s fee=%s → pool_acb=%s pool_units=%s acb/unit=%s",
        event.amount,
        event.asset,
        cost,
        fee,
        new_total_acb,
        new_units,
        new_acb_per_unit,
    )

    return ACBState(
        asset=pool.asset,
        total_units=new_units,
        total_acb_cad=new_total_acb,
        acb_per_unit_cad=new_acb_per_unit,
    )


def _process_disposal(
    event: ClassifiedEvent,
    pool: ACBState,
    acq_years: set[int],
) -> tuple[Disposition, ACBState]:
    """Calculate gain/loss for a disposal event and return (Disposition, new pool).

    On disposition:
        cost_of_units_sold = ACB_per_unit x units_sold
        capital_gain_loss = proceeds_CAD - cost_of_units_sold - selling_fees_CAD
        remaining_total_ACB = previous_total_ACB - cost_of_units_sold
        remaining_units = previous_total_units - units_sold
    """
    units = event.amount
    proceeds = event.cad_proceeds or Decimal("0")
    selling_fees = event.cad_fee or Decimal("0")

    # Guard against over-disposal — can indicate missing import data.
    tolerance = Decimal("0.00000001")  # allow for tiny floating-point drift
    if units > pool.total_units + tolerance:
        raise InsufficientUnitsError(
            f"Cannot dispose of {units} {event.asset}: "
            f"pool only holds {pool.total_units} units. "
            "Check that all buy transactions have been imported."
        )

    # Clamp to pool size to handle dust-level rounding.
    units = min(units, pool.total_units)

    acb_of_disposed = (pool.acb_per_unit_cad * units).quantize(_ROUNDING)
    gain_loss = proceeds - acb_of_disposed - selling_fees

    remaining_acb = pool.total_acb_cad - acb_of_disposed
    remaining_units = pool.total_units - units

    # Guard against negative pool from rounding.
    remaining_acb = max(remaining_acb, Decimal("0"))
    remaining_units = max(remaining_units, Decimal("0"))

    new_acb_per_unit = (
        (remaining_acb / remaining_units).quantize(_ROUNDING)
        if remaining_units > Decimal("0")
        else Decimal("0")
    )

    new_pool = ACBState(
        asset=pool.asset,
        total_units=remaining_units,
        total_acb_cad=remaining_acb,
        acb_per_unit_cad=new_acb_per_unit,
    )

    year_acquired = _format_year_acquired(acq_years, event.timestamp)

    disp = Disposition(
        id=0,  # assigned by repository after INSERT
        event_id=event.id,
        timestamp=event.timestamp,
        asset=event.asset,
        units=units,
        proceeds_cad=proceeds,
        acb_of_disposed_cad=acb_of_disposed,
        selling_fees_cad=selling_fees,
        gain_loss_cad=gain_loss,
        disposition_type=_map_disposition_type(event.event_type),
        year_acquired=year_acquired,
        acb_state_before=pool,
        acb_state_after=new_pool,
    )

    logger.debug(
        "acb: sell %s %s — proceeds=%s acb_disposed=%s fees=%s gain_loss=%s",
        units,
        event.asset,
        proceeds,
        acb_of_disposed,
        selling_fees,
        gain_loss,
    )

    return disp, new_pool


def _format_year_acquired(acq_years: set[int], disposal_ts: datetime) -> str:
    """Return the year-acquired string for Schedule 3 Column 1.

    - Single acquisition year → that year as a string.
    - Multiple years → "Various".
    - No acquisition years tracked (edge case) → "Unknown".
    """
    if not acq_years:
        return "Unknown"
    if len(acq_years) == 1:
        return str(next(iter(acq_years)))
    return "Various"


def _map_disposition_type(event_type: str) -> str:
    """Map a ``ClassifiedEvent.event_type`` to a disposition_type string."""
    mapping: dict[str, str] = {
        "sell": "sell",
        "fee_disposal": "fee_disposal",
        "spend": "spend",
    }
    return mapping.get(event_type, "sell")
