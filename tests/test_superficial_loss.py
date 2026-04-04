"""Unit tests for the superficial loss detector (src/sirop/engine/superficial_loss.py).

Test cases cover the five scenarios listed in CLAUDE.md plan:
- No repurchase → pass through
- Full repurchase → full denial
- Fewer units repurchased → partial denial
- Gain → never triggers
- Window boundary (day 30 triggers, day 31 doesn't)
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sirop.engine.acb import TaxRules
from sirop.engine.superficial_loss import run
from sirop.models.disposition import ACBState, Disposition
from sirop.models.event import ClassifiedEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)

_DEFAULT_RULES = TaxRules(
    capital_gains_inclusion_rate=Decimal("0.50"),
    superficial_loss_window_days=30,
    reward_treatment={},
)

_POOL_BEFORE = ACBState(
    asset="BTC",
    total_units=Decimal("1.0"),
    total_acb_cad=Decimal("50000"),
    acb_per_unit_cad=Decimal("50000"),
)


def _make_loss_disposition(
    units: str = "0.5",
    proceeds: str = "20000",
    acb_disposed: str = "25000",
    ts: datetime | None = None,
    disp_id: int = 1,
) -> Disposition:
    stamp = ts or _BASE_TS
    units_dec = Decimal(units)
    gain = Decimal(proceeds) - Decimal(acb_disposed)
    # Derive the after-pool state from the before-pool state and units sold.
    remaining_units = _POOL_BEFORE.total_units - units_dec
    remaining_acb = _POOL_BEFORE.total_acb_cad - Decimal(acb_disposed)
    pool_after = ACBState(
        asset="BTC",
        total_units=remaining_units,
        total_acb_cad=remaining_acb,
        acb_per_unit_cad=(
            (remaining_acb / remaining_units).quantize(Decimal("0.00000001"))
            if remaining_units > Decimal("0")
            else Decimal("0")
        ),
    )
    return Disposition(
        id=disp_id,
        event_id=10,
        timestamp=stamp,
        asset="BTC",
        units=units_dec,
        proceeds_cad=Decimal(proceeds),
        acb_of_disposed_cad=Decimal(acb_disposed),
        selling_fees_cad=Decimal("0"),
        gain_loss_cad=gain,
        disposition_type="sell",
        year_acquired="2025",
        acb_state_before=_POOL_BEFORE,
        acb_state_after=pool_after,
    )


def _make_gain_disposition(disp_id: int = 1) -> Disposition:
    # Sell 0.5 of the 1.0 BTC pool at a gain.
    remaining_units = _POOL_BEFORE.total_units - Decimal("0.5")
    remaining_acb = _POOL_BEFORE.total_acb_cad - Decimal("25000")
    pool_after = ACBState(
        asset="BTC",
        total_units=remaining_units,
        total_acb_cad=remaining_acb,
        acb_per_unit_cad=(remaining_acb / remaining_units).quantize(Decimal("0.00000001")),
    )
    return Disposition(
        id=disp_id,
        event_id=10,
        timestamp=_BASE_TS,
        asset="BTC",
        units=Decimal("0.5"),
        proceeds_cad=Decimal("30000"),
        acb_of_disposed_cad=Decimal("25000"),
        selling_fees_cad=Decimal("0"),
        gain_loss_cad=Decimal("5000"),
        disposition_type="sell",
        year_acquired="2025",
        acb_state_before=_POOL_BEFORE,
        acb_state_after=pool_after,
    )


def _buy_event(offset_days: int, amount: str = "0.5", event_id: int = 20) -> ClassifiedEvent:
    return ClassifiedEvent(
        id=event_id,
        vtx_id=event_id,
        timestamp=_BASE_TS + timedelta(days=offset_days),
        event_type="buy",
        asset="BTC",
        amount=Decimal(amount),
        cad_proceeds=None,
        cad_cost=Decimal("20000"),
        cad_fee=None,
        txid=None,
        source="test",
        is_taxable=True,
    )


def _sell_event(offset_days: int, amount: str = "0.5", event_id: int = 10) -> ClassifiedEvent:
    return ClassifiedEvent(
        id=event_id,
        vtx_id=event_id,
        timestamp=_BASE_TS + timedelta(days=offset_days),
        event_type="sell",
        asset="BTC",
        amount=Decimal(amount),
        cad_proceeds=Decimal("20000"),
        cad_cost=None,
        cad_fee=None,
        txid=None,
        source="test",
        is_taxable=True,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_repurchase_no_sld() -> None:
    """A loss with no repurchase in the 61-day window passes through unchanged."""
    disp = _make_loss_disposition()
    # Only the sell event — no buy in the window.
    events = [_sell_event(offset_days=0)]

    results = run([disp], events, _DEFAULT_RULES)
    assert len(results) == 1
    adj = results[0]

    assert not adj.is_superficial_loss
    assert adj.superficial_loss_denied_cad == Decimal("0")
    assert adj.adjusted_gain_loss_cad == disp.gain_loss_cad
    assert adj.allowable_loss_cad == abs(disp.gain_loss_cad)
    assert adj.adjusted_acb_of_repurchase_cad is None


def test_full_sld_full_repurchase() -> None:
    """Full repurchase of same quantity in the window → full denial."""
    disp = _make_loss_disposition(units="0.5", proceeds="20000", acb_disposed="25000")
    # Buy the same 0.5 BTC within the 30-day window.
    events = [
        _sell_event(offset_days=0, amount="0.5"),  # the disposal
        _buy_event(offset_days=15, amount="0.5"),  # repurchase within window
    ]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert adj.is_superficial_loss
    # Total loss = 5000, superficial portion = min(0.5, 0.5, balance) = 0.5
    # denied = 5000 x (0.5 / 0.5) = 5000
    assert adj.superficial_loss_denied_cad == Decimal("5000")
    assert adj.allowable_loss_cad == Decimal("0")
    assert adj.adjusted_gain_loss_cad == Decimal("0")
    assert adj.adjusted_acb_of_repurchase_cad == Decimal("5000")


def test_partial_sld_fewer_units_repurchased() -> None:
    """Fewer units repurchased than sold → partial denial."""
    # Sell 1.0 BTC at a loss, repurchase only 0.3 BTC.
    disp = _make_loss_disposition(units="1.0", proceeds="40000", acb_disposed="50000")
    events = [
        _sell_event(offset_days=0, amount="1.0"),
        _buy_event(offset_days=10, amount="0.3"),  # partial repurchase
    ]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert adj.is_superficial_loss
    total_loss = Decimal("10000")
    # superficial_portion = min(1.0, 0.3, balance_at_end) = 0.3
    # denied = 10000 x (0.3 / 1.0) = 3000
    assert adj.superficial_loss_denied_cad == Decimal("3000")
    assert adj.allowable_loss_cad == Decimal("7000")
    # adjusted_gain_loss = -(7000) — a loss of 7000 is still allowed
    assert adj.adjusted_gain_loss_cad == Decimal("-7000")
    assert adj.adjusted_acb_of_repurchase_cad == Decimal("3000")
    assert adj.superficial_loss_denied_cad + adj.allowable_loss_cad == total_loss


def test_gain_not_superficial_loss() -> None:
    """A gain never triggers the superficial loss rule."""
    disp = _make_gain_disposition()
    # Even with a repurchase, gains are untouched.
    events = [
        _sell_event(offset_days=0),
        _buy_event(offset_days=5),
    ]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert not adj.is_superficial_loss
    assert adj.adjusted_gain_loss_cad == Decimal("5000")
    assert adj.adjusted_acb_of_repurchase_cad is None


def test_sld_window_boundary_day_30_triggers() -> None:
    """A repurchase exactly on day 30 falls within the window and triggers SLD."""
    disp = _make_loss_disposition(units="0.5", proceeds="20000", acb_disposed="25000")
    events = [
        _sell_event(offset_days=0, amount="0.5"),
        _buy_event(offset_days=30, amount="0.5"),  # exactly at boundary
    ]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert adj.is_superficial_loss


def test_sld_window_boundary_day_31_does_not_trigger() -> None:
    """A repurchase on day 31 falls outside the window — SLD does not apply."""
    disp = _make_loss_disposition(units="0.5", proceeds="20000", acb_disposed="25000")
    events = [
        _sell_event(offset_days=0, amount="0.5"),
        _buy_event(offset_days=31, amount="0.5"),  # just outside boundary
    ]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert not adj.is_superficial_loss
    assert adj.allowable_loss_cad == Decimal("5000")


def test_passthrough_preserves_gain_loss_sign() -> None:
    """For a non-superficial loss, adjusted_gain_loss equals the original loss."""
    disp = _make_loss_disposition(units="0.5", proceeds="20000", acb_disposed="25000")
    # No repurchase.
    events = [_sell_event(offset_days=0)]

    results = run([disp], events, _DEFAULT_RULES)
    adj = results[0]

    assert adj.adjusted_gain_loss_cad == Decimal("-5000")
    assert adj.allowable_loss_cad == Decimal("5000")


def test_multiple_dispositions_independent() -> None:
    """Each disposition is evaluated independently against the full event list."""
    disp1 = _make_loss_disposition(
        units="0.5", proceeds="20000", acb_disposed="25000", ts=_BASE_TS, disp_id=1
    )
    disp2 = _make_loss_disposition(
        units="0.2",
        proceeds="8000",
        acb_disposed="10000",
        ts=_BASE_TS + timedelta(days=60),
        disp_id=2,
    )
    events = [
        _sell_event(offset_days=0, amount="0.5", event_id=10),
        _buy_event(offset_days=10, amount="0.5", event_id=20),  # triggers SLD for disp1
        _sell_event(offset_days=60, amount="0.2", event_id=11),  # disp2
        # No repurchase near disp2 → disp2 is NOT superficial
    ]

    results = run([disp1, disp2], events, _DEFAULT_RULES)

    adj1 = next(r for r in results if r.disposition_id == 1)
    adj2 = next(r for r in results if r.disposition_id == 2)  # noqa: PLR2004

    assert adj1.is_superficial_loss
    assert not adj2.is_superficial_loss
