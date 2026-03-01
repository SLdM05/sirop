"""Superficial loss detector — pipeline stage 6 (PURE).

Applies the CRA superficial loss rule (Section 54 of the Income Tax Act)
to the output of the ACB engine.

This module is PURE: it receives typed dataclasses and returns typed
dataclasses.  It never imports from ``src/db/``, reads files, calls APIs,
or accesses config.  All dependencies are injected by the pipeline
coordinator.

Rule summary (CLAUDE.md §4 / tax ref §4)
-----------------------------------------
A capital loss is superficial — and therefore denied — when ALL of the
following hold:

1. You sell crypto at a **loss**.
2. You (or an affiliated person) acquire the **same cryptocurrency** during
   the period from 30 days before to 30 days after the sale (61-day window).
3. At the end of the 30th day after the sale, you or the affiliated person
   **still hold** identical property.

When triggered, the denied portion is added to the ACB of the repurchased
units (loss deferral, not destruction).

Partial denial applies when fewer units were repurchased than sold:
    denied = total_loss x (min(sold, repurchased, held_at_end) / sold)
    allowable = total_loss - denied
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.models.disposition import AdjustedDisposition, Disposition
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    from sirop.engine.acb import TaxRules
    from sirop.models.event import ClassifiedEvent

logger = get_logger(__name__)

# Event types that represent acquisitions (increase the holding balance).
_ACQUISITION_TYPES = frozenset({"buy", "income"})

# Event types that represent disposals (decrease the holding balance).
_DISPOSAL_TYPES = frozenset({"sell", "fee_disposal", "spend"})


def run(
    dispositions: list[Disposition],
    events: list[ClassifiedEvent],
    tax_rules: TaxRules,
) -> list[AdjustedDisposition]:
    """Apply the superficial loss rule to all *dispositions*.

    Parameters
    ----------
    dispositions:
        Output of the ACB engine (``repositories.read_dispositions()``).
    events:
        ALL classified events (both taxable and non-taxable, from
        ``repositories.read_all_classified_events()``).  Used to compute
        running balances and find repurchases in the 61-day window.
    tax_rules:
        Tax parameters for the batch year (provides ``superficial_loss_window_days``).

    Returns
    -------
    list[AdjustedDisposition]
        One ``AdjustedDisposition`` per input ``Disposition``, in the same order.
        Non-loss disposals are passed through unchanged.
    """
    window_days = tax_rules.superficial_loss_window_days
    # Pre-sort events chronologically for the balance helper.
    sorted_events = sorted(events, key=lambda e: e.timestamp)

    results: list[AdjustedDisposition] = []
    sld_count = 0

    for disp in sorted(dispositions, key=lambda d: d.timestamp):
        adj = _apply_rule(disp, sorted_events, window_days)
        results.append(adj)
        if adj.is_superficial_loss:
            sld_count += 1
            logger.warning(
                "%s on %s is superficial — denied %s CAD, allowable %s CAD",
                disp.asset,
                disp.timestamp.date(),
                adj.superficial_loss_denied_cad,
                adj.allowable_loss_cad,
            )

    logger.info(
        "%d disposition(s) examined, %d superficial loss(es) found",
        len(dispositions),
        sld_count,
    )
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_rule(
    disp: Disposition,
    sorted_events: list[ClassifiedEvent],
    window_days: int,
) -> AdjustedDisposition:
    """Evaluate the superficial loss rule for a single *disp*.

    Returns an ``AdjustedDisposition`` with the rule applied (or passed
    through unchanged if the rule does not trigger).
    """
    # Only losses can be superficial.
    if disp.gain_loss_cad >= Decimal("0"):
        return _passthrough(disp)

    window_start = disp.timestamp - timedelta(days=window_days)
    window_end = disp.timestamp + timedelta(days=window_days)

    # Count units of the same asset acquired in the 61-day window.
    units_acquired = _units_acquired_in_window(
        disp.asset, disp.timestamp, sorted_events, window_start, window_end
    )

    if units_acquired <= Decimal("0"):
        # No repurchase → rule cannot apply.
        return _passthrough(disp)

    # Estimate units held at the end of the window.
    # Start from the authoritative pool balance immediately after the disposal
    # (from the ACB engine), then apply any events that occur after the disposal
    # and on or before window_end.
    units_at_end = _balance_at_window_end(
        disp.asset,
        disp.timestamp,
        window_end,
        sorted_events,
        disp.acb_state_after.total_units,
    )

    if units_at_end <= Decimal("0"):
        # All units were sold by end of window → rule does not apply.
        return _passthrough(disp)

    # Superficial loss rule triggers.
    units_sold = disp.units
    superficial_portion = min(units_sold, units_acquired, units_at_end)
    total_loss = abs(disp.gain_loss_cad)

    denied = (total_loss * (superficial_portion / units_sold)).quantize(Decimal("0.01"))
    allowable = total_loss - denied

    logger.debug(
        "superficial_loss: %s — sold=%s acquired_in_window=%s held_at_end=%s "
        "portion=%s denied=%s allowable=%s",
        disp.asset,
        units_sold,
        units_acquired,
        units_at_end,
        superficial_portion,
        denied,
        allowable,
    )

    return AdjustedDisposition(
        id=0,  # assigned by repository after INSERT
        disposition_id=disp.id,
        timestamp=disp.timestamp,
        asset=disp.asset,
        units=disp.units,
        proceeds_cad=disp.proceeds_cad,
        acb_of_disposed_cad=disp.acb_of_disposed_cad,
        selling_fees_cad=disp.selling_fees_cad,
        gain_loss_cad=disp.gain_loss_cad,
        is_superficial_loss=True,
        superficial_loss_denied_cad=denied,
        allowable_loss_cad=allowable,
        adjusted_gain_loss_cad=-allowable,  # negative = allowable loss
        adjusted_acb_of_repurchase_cad=denied,
        disposition_type=disp.disposition_type,
        year_acquired=disp.year_acquired,
    )


def _passthrough(disp: Disposition) -> AdjustedDisposition:
    """Return an ``AdjustedDisposition`` that mirrors *disp* with no SLD applied."""
    # For a gain, adjusted_gain_loss = gain_loss (positive).
    # For a non-superficial loss, adjusted_gain_loss = gain_loss (negative, fully allowable).
    allowable = Decimal("0") if disp.gain_loss_cad >= Decimal("0") else abs(disp.gain_loss_cad)
    return AdjustedDisposition(
        id=0,
        disposition_id=disp.id,
        timestamp=disp.timestamp,
        asset=disp.asset,
        units=disp.units,
        proceeds_cad=disp.proceeds_cad,
        acb_of_disposed_cad=disp.acb_of_disposed_cad,
        selling_fees_cad=disp.selling_fees_cad,
        gain_loss_cad=disp.gain_loss_cad,
        is_superficial_loss=False,
        superficial_loss_denied_cad=Decimal("0"),
        allowable_loss_cad=allowable,
        adjusted_gain_loss_cad=disp.gain_loss_cad,
        adjusted_acb_of_repurchase_cad=None,
        disposition_type=disp.disposition_type,
        year_acquired=disp.year_acquired,
    )


def _units_acquired_in_window(
    asset: str,
    sale_timestamp: object,
    sorted_events: list[ClassifiedEvent],
    window_start: object,
    window_end: object,
) -> Decimal:
    """Sum units of *asset* acquired (buy/income) in the 61-day window.

    The sale event itself is excluded from the count.
    """
    total = Decimal("0")
    for evt in sorted_events:
        if evt.asset != asset:
            continue
        if evt.event_type not in _ACQUISITION_TYPES:
            continue
        if not (window_start <= evt.timestamp <= window_end):  # type: ignore[operator]
            continue
        # Exclude events at the exact same timestamp as the disposal
        # to avoid the disposal itself being counted as a repurchase.
        if evt.timestamp == sale_timestamp:
            continue
        total += evt.amount
    return total


def _balance_at_window_end(
    asset: str,
    disposal_ts: object,
    window_end: object,
    sorted_events: list[ClassifiedEvent],
    starting_balance: Decimal,
) -> Decimal:
    """Return the holding balance of *asset* at *window_end*.

    Starts from *starting_balance* (the authoritative pool state produced by
    the ACB engine immediately after the disposal at *disposal_ts*) and applies
    only events that occur **strictly after** *disposal_ts* and on or before
    *window_end*.  This avoids reprocessing the disposal itself and any earlier
    history that is already captured in *starting_balance*.

    A negative result is clamped to zero.
    """
    balance = starting_balance
    for evt in sorted_events:
        if evt.asset != asset:
            continue
        if evt.timestamp <= disposal_ts:  # type: ignore[operator]
            continue
        if evt.timestamp > window_end:  # type: ignore[operator]
            break
        if evt.event_type in _ACQUISITION_TYPES:
            balance += evt.amount
        elif evt.event_type in _DISPOSAL_TYPES:
            balance -= evt.amount
    return max(balance, Decimal("0"))


def _running_balance(
    asset: str,
    as_of: object,
    sorted_events: list[ClassifiedEvent],
) -> Decimal:
    """Estimate the holding balance of *asset* at *as_of*.

    Sums all acquisitions minus all disposals up to and including *as_of*.
    Uses only classified events (taxable and non-taxable acquisitions /
    disposals) — does not model full portfolio history.

    A negative result is clamped to zero (data quality issue).
    """
    balance = Decimal("0")
    for evt in sorted_events:
        if evt.asset != asset:
            continue
        if evt.timestamp > as_of:  # type: ignore[operator]
            break
        if evt.event_type in _ACQUISITION_TYPES:
            balance += evt.amount
        elif evt.event_type in _DISPOSAL_TYPES:
            balance -= evt.amount
    return max(balance, Decimal("0"))
