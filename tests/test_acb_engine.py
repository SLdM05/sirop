"""Unit tests for the ACB engine (src/sirop/engine/acb.py).

Known-answer test cases drawn from:
- CLAUDE.md §3 worked example
- docs/ref/crypto-tax-reference-quebec-2025.md §3 worked example

All values use Decimal to match production code.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sirop.engine.acb import PoolUnderrun, TaxRules, run
from sirop.models.event import ClassifiedEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

_DEFAULT_RULES = TaxRules(
    capital_gains_inclusion_rate=Decimal("0.50"),
    superficial_loss_window_days=30,
)


def _buy(  # noqa: PLR0913
    asset: str,
    amount: str,
    cad_cost: str,
    cad_fee: str = "0",
    offset_days: int = 0,
    event_id: int = 1,
) -> ClassifiedEvent:
    return ClassifiedEvent(
        id=event_id,
        vtx_id=event_id,
        timestamp=_BASE_TS + timedelta(days=offset_days),
        event_type="buy",
        asset=asset,
        amount=Decimal(amount),
        cad_proceeds=None,
        cad_cost=Decimal(cad_cost),
        cad_fee=Decimal(cad_fee) if cad_fee != "0" else None,
        txid=None,
        source="test",
        is_taxable=True,
    )


def _sell(  # noqa: PLR0913
    asset: str,
    amount: str,
    cad_proceeds: str,
    cad_fee: str = "0",
    offset_days: int = 0,
    event_id: int = 2,
) -> ClassifiedEvent:
    return ClassifiedEvent(
        id=event_id,
        vtx_id=event_id,
        timestamp=_BASE_TS + timedelta(days=offset_days),
        event_type="sell",
        asset=asset,
        amount=Decimal(amount),
        cad_proceeds=Decimal(cad_proceeds),
        cad_cost=None,
        cad_fee=Decimal(cad_fee) if cad_fee != "0" else None,
        txid=None,
        source="test",
        is_taxable=True,
    )


# ---------------------------------------------------------------------------
# Tests — from tax reference §3 worked example
# ---------------------------------------------------------------------------


def test_buy_buy_sell_gain() -> None:
    """Two buys then a sell — verifies the §3 worked example.

    Step 1: Buy 0.5 BTC for $25,000 + $50 fee → ACB = $25,050, ACB/unit = $50,100
    Step 2: Buy 0.3 BTC for $18,000 + $36 fee → ACB = $43,086, ACB/unit = $53,857.50
    Step 3: Sell 0.4 BTC for $28,000 with $40 fee
        ACB of disposed = $53,857.50 x 0.4 = $21,543
        gain = $28,000 - $21,543 - $40 = $6,417
    """
    events = [
        _buy("BTC", "0.5", "25000", cad_fee="50", offset_days=0, event_id=1),
        _buy("BTC", "0.3", "18000", cad_fee="36", offset_days=10, event_id=2),
        _sell("BTC", "0.4", "28000", cad_fee="40", offset_days=20, event_id=3),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)

    assert len(disps) == 1
    d = disps[0]

    # ACB of disposed = ACB/unit after step 2 x 0.4
    # ACB after buy 2 = (25050 + 18036) / 0.8 = 53857.50
    expected_acb_per_unit = Decimal("53857.50")
    expected_acb_of_disposed = (expected_acb_per_unit * Decimal("0.4")).quantize(
        Decimal("0.00000001")
    )
    expected_gain = Decimal("28000") - expected_acb_of_disposed - Decimal("40")

    assert d.gain_loss_cad == expected_gain
    assert d.asset == "BTC"
    assert d.units == Decimal("0.4")
    assert d.proceeds_cad == Decimal("28000")
    assert d.selling_fees_cad == Decimal("40")


def test_sell_at_loss() -> None:
    """Sell below ACB → negative gain_loss."""
    events = [
        _buy("BTC", "1.0", "50000", cad_fee="0", offset_days=0, event_id=1),
        _sell("BTC", "0.5", "20000", cad_fee="0", offset_days=30, event_id=2),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)

    assert len(disps) == 1
    d = disps[0]

    # ACB/unit = 50000 → ACB of disposed = 25000
    # gain = 20000 - 25000 - 0 = -5000
    assert d.gain_loss_cad == Decimal("-5000")
    assert d.acb_of_disposed_cad == Decimal("25000.00000000")


def test_fee_disposal_reduces_pool() -> None:
    """A fee_disposal event removes units and records a gain/loss."""
    events = [
        _buy("BTC", "1.0", "60000", offset_days=0, event_id=1),
        ClassifiedEvent(
            id=2,
            vtx_id=2,
            timestamp=_BASE_TS + timedelta(days=5),
            event_type="fee_disposal",
            asset="BTC",
            amount=Decimal("0.0001"),
            cad_proceeds=Decimal("6"),  # BTC at ~60000 x 0.0001
            cad_cost=None,
            cad_fee=None,
            txid=None,
            source="test",
            is_taxable=True,
        ),
    ]
    disps, states, *_ = run(events, _DEFAULT_RULES)

    assert len(disps) == 1
    d = disps[0]
    assert d.disposition_type == "fee_disposal"
    assert d.units == Decimal("0.0001")
    # ACB of disposed = 60000 x 0.0001 = 6.00
    assert d.acb_of_disposed_cad == Decimal("6.00000000")
    # Pool should have 0.9999 units remaining.
    assert states[0].total_units == Decimal("0.9999")


def test_multiple_assets_isolated() -> None:
    """BTC and ETH pools remain independent."""
    events = [
        _buy("BTC", "1.0", "60000", offset_days=0, event_id=1),
        _buy("ETH", "10.0", "30000", offset_days=1, event_id=2),
        _sell("BTC", "0.5", "32000", offset_days=10, event_id=3),
        _sell("ETH", "5.0", "18000", offset_days=11, event_id=4),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)

    assert len(disps) == 2  # noqa: PLR2004

    btc_disp = next(d for d in disps if d.asset == "BTC")
    eth_disp = next(d for d in disps if d.asset == "ETH")

    # BTC: ACB/unit = 60000, sold 0.5 → ACB disposed = 30000, gain = 32000 - 30000 = 2000
    assert btc_disp.gain_loss_cad == Decimal("2000.00000000")

    # ETH: ACB/unit = 3000, sold 5 → ACB disposed = 15000, gain = 18000 - 15000 = 3000
    assert eth_disp.gain_loss_cad == Decimal("3000.00000000")


def test_empty_pool_disposal_clamps_and_warns() -> None:
    """Disposing from an empty pool clamps to 0 and returns a PoolUnderrun warning.

    Previously this raised InsufficientUnitsError and crashed the pipeline.
    Now the engine records the underrun and continues so the user gets output
    along with a W005 warning.
    """
    events = [
        _sell("BTC", "0.1", "6000", offset_days=0, event_id=1),
    ]
    disps, _, _, _, underruns = run(events, _DEFAULT_RULES)
    assert len(underruns) == 1
    u = underruns[0]
    assert isinstance(u, PoolUnderrun)
    assert u.asset == "BTC"
    assert u.attempted == Decimal("0.1")
    assert u.available == Decimal("0")
    # Disposal is still recorded (clamped to 0 units disposed).
    assert len(disps) == 1


def test_no_underrun_on_normal_disposal() -> None:
    """A disposal within pool balance produces no underrun warning."""
    events = [
        _buy("BTC", "0.5", "30000", offset_days=0, event_id=1),
        _sell("BTC", "0.3", "20000", offset_days=1, event_id=2),
    ]
    _, _, _, _, underruns = run(events, _DEFAULT_RULES)
    assert underruns == []


def test_disposition_type_recorded() -> None:
    """disposition_type is carried correctly from event_type."""
    events = [
        _buy("BTC", "1.0", "60000", offset_days=0, event_id=1),
        _sell("BTC", "0.1", "7000", offset_days=10, event_id=2),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)
    assert disps[0].disposition_type == "sell"


def test_year_acquired_single_year() -> None:
    """year_acquired is the acquisition year when all buys are in one year."""
    events = [
        _buy("BTC", "1.0", "60000", offset_days=0, event_id=1),
        _sell("BTC", "0.5", "35000", offset_days=100, event_id=2),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)
    assert disps[0].year_acquired == "2025"


def test_year_acquired_various() -> None:
    """year_acquired is 'Various' when acquisitions span multiple calendar years."""
    events = [
        ClassifiedEvent(
            id=1,
            vtx_id=1,
            timestamp=datetime(2024, 6, 1, tzinfo=UTC),
            event_type="buy",
            asset="BTC",
            amount=Decimal("0.5"),
            cad_proceeds=None,
            cad_cost=Decimal("30000"),
            cad_fee=None,
            txid=None,
            source="test",
            is_taxable=True,
        ),
        ClassifiedEvent(
            id=2,
            vtx_id=2,
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            event_type="buy",
            asset="BTC",
            amount=Decimal("0.5"),
            cad_proceeds=None,
            cad_cost=Decimal("30000"),
            cad_fee=None,
            txid=None,
            source="test",
            is_taxable=True,
        ),
        ClassifiedEvent(
            id=3,
            vtx_id=3,
            timestamp=datetime(2025, 6, 1, tzinfo=UTC),
            event_type="sell",
            asset="BTC",
            amount=Decimal("0.5"),
            cad_proceeds=Decimal("35000"),
            cad_cost=None,
            cad_fee=None,
            txid=None,
            source="test",
            is_taxable=True,
        ),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)
    assert disps[0].year_acquired == "Various"


def test_income_event_establishes_acb() -> None:
    """An income event adds to the ACB pool at its FMV."""
    events = [
        ClassifiedEvent(
            id=1,
            vtx_id=1,
            timestamp=_BASE_TS,
            event_type="income",
            asset="BTC",
            amount=Decimal("0.001"),
            cad_proceeds=None,
            cad_cost=Decimal("65"),  # staking reward FMV
            cad_fee=None,
            txid=None,
            source="test",
            is_taxable=True,
        ),
        _sell("BTC", "0.001", "70", offset_days=30, event_id=2),
    ]
    disps, *_ = run(events, _DEFAULT_RULES)

    assert len(disps) == 1
    d = disps[0]
    # ACB/unit = 65000 (65 / 0.001)
    # ACB = 65 (FMV of 0.001 BTC), units = 0.001, ACB/unit = 65000
    # ACB disposed = 65000 x 0.001 = 65
    assert d.acb_of_disposed_cad == Decimal("65.00000000")
    # gain = 70 - 65 = 5
    assert d.gain_loss_cad == Decimal("5.00000000")


def test_final_pools_returned_for_acquisition_only_asset() -> None:
    """Assets never sold return their final pool state in the third return value.

    This is the data source for year-end holdover acb_state snapshots: assets
    that were only bought/received in the tax year produce no acb_states entries
    (those are only written after disposals), so the caller must use final_pools
    to write year-end holdings for them.
    """
    events = [
        _buy("BTC", "0.5", "25000", cad_fee="50", offset_days=0, event_id=1),
        _buy("BTC", "0.3", "18000", cad_fee="36", offset_days=10, event_id=2),
    ]
    disps, acb_states, final_pools, last_events, _ = run(events, _DEFAULT_RULES)

    # No disposals → no dispositions and no per-disposal acb_state entries.
    assert len(disps) == 0
    assert len(acb_states) == 0

    # Final pool must reflect both buys.
    assert "BTC" in final_pools
    pool = final_pools["BTC"]
    assert pool.total_units == Decimal("0.8")
    assert pool.total_acb_cad == Decimal("25000") + Decimal("50") + Decimal("18000") + Decimal("36")

    # last_events must point to the second (most recent) buy (event_id=2).
    assert "BTC" in last_events
    assert last_events["BTC"].id == events[1].id
