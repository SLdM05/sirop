"""Known-answer tests for sirop.reports.formatter.build_report.

All fixture data is synthetic — no real txids, amounts, or addresses.
"""

from datetime import UTC, datetime
from decimal import Decimal

from sirop.models.disposition import ACBState, AdjustedDisposition, IncomeEvent
from sirop.models.event import ClassifiedEvent
from sirop.reports.formatter import build_detail_report, build_report

_UTC = UTC

# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _adj_disp(  # noqa: PLR0913
    *,
    id: int,
    timestamp: datetime,
    asset: str = "BTC",
    units: str,
    proceeds: str,
    acb: str,
    fees: str,
    gain_loss: str,
    adjusted_gain_loss: str,
    is_sl: bool = False,
    denied: str = "0",
    allowable: str = "0",
    adjusted_acb_repurchase: str | None = None,
    disposition_type: str = "sell",
    year_acquired: str = "Various",
) -> AdjustedDisposition:
    return AdjustedDisposition(
        id=id,
        disposition_id=id,
        timestamp=timestamp,
        asset=asset,
        units=Decimal(units),
        proceeds_cad=Decimal(proceeds),
        acb_of_disposed_cad=Decimal(acb),
        selling_fees_cad=Decimal(fees),
        gain_loss_cad=Decimal(gain_loss),
        is_superficial_loss=is_sl,
        superficial_loss_denied_cad=Decimal(denied),
        allowable_loss_cad=Decimal(allowable),
        adjusted_gain_loss_cad=Decimal(adjusted_gain_loss),
        adjusted_acb_of_repurchase_cad=Decimal(adjusted_acb_repurchase)
        if adjusted_acb_repurchase
        else None,
        disposition_type=disposition_type,
        year_acquired=year_acquired,
    )


def _income(  # noqa: PLR0913
    *,
    id: int,
    timestamp: datetime,
    asset: str = "BTC",
    units: str,
    income_type: str = "staking",
    fmv_cad: str,
    source: str = "test-wallet",
) -> IncomeEvent:
    return IncomeEvent(
        id=id,
        vtx_id=id,
        timestamp=timestamp,
        asset=asset,
        units=Decimal(units),
        income_type=income_type,
        fmv_cad=Decimal(fmv_cad),
        source=source,
    )


def _acb_state(
    *,
    asset: str = "BTC",
    units: str,
    pool_cost: str,
) -> ACBState:
    u = Decimal(units)
    c = Decimal(pool_cost)
    return ACBState(
        asset=asset,
        total_units=u,
        total_acb_cad=c,
        acb_per_unit_cad=c / u if u else Decimal("0"),
    )


def _acquisition(  # noqa: PLR0913
    *,
    id: int,
    timestamp: datetime,
    asset: str = "BTC",
    amount: str,
    cad_cost: str,
    cad_fee: str = "0",
) -> ClassifiedEvent:
    return ClassifiedEvent(
        id=id,
        vtx_id=id,
        timestamp=timestamp,
        event_type="buy",
        asset=asset,
        amount=Decimal(amount),
        cad_proceeds=None,
        cad_cost=Decimal(cad_cost),
        cad_fee=Decimal(cad_fee),
        txid=None,
        source="test-exchange",
        is_taxable=True,
        wallet_id=None,
    )


# ---------------------------------------------------------------------------
# Test 1: two dispositions (gain + loss), no superficial loss
# ---------------------------------------------------------------------------


def test_federal_dispositions_table_two_rows() -> None:
    d1 = _adj_disp(
        id=1,
        timestamp=datetime(2025, 3, 15, 14, 0, 0, tzinfo=_UTC),
        units="0.1",
        proceeds="5000",
        acb="3000",
        fees="50",
        gain_loss="1950",
        adjusted_gain_loss="1950",
    )
    d2 = _adj_disp(
        id=2,
        timestamp=datetime(2025, 7, 20, 10, 0, 0, tzinfo=_UTC),
        units="0.05",
        proceeds="2000",
        acb="2500",
        fees="25",
        gain_loss="-525",
        adjusted_gain_loss="-525",
    )

    report = build_report(
        dispositions=[d1, d2],
        income_events=[],
        acb_final=[],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    assert "# sirop Tax Report \u2014 test2025 (Tax Year 2025)" in report
    assert "5,000.00" in report
    assert "1,950.00" in report
    assert "-525.00" in report
    # Net = 1950 - 525 = 1425
    assert "1,425.00" in report
    # Taxable = 1425 * 0.5 = 712.50
    assert "712.50" in report
    # No superficial loss section
    assert "Superficial Losses" not in report
    assert "T1 Line 12700" in report


# ---------------------------------------------------------------------------
# Test 2: superficial loss section appears
# ---------------------------------------------------------------------------


def test_superficial_loss_section_appears() -> None:
    d1 = _adj_disp(
        id=1,
        timestamp=datetime(2025, 5, 1, 9, 0, 0, tzinfo=_UTC),
        units="0.2",
        proceeds="8000",
        acb="10000",
        fees="100",
        gain_loss="-2100",
        adjusted_gain_loss="0",
        is_sl=True,
        denied="2100",
        allowable="0",
    )

    summary = build_report(
        dispositions=[d1],
        income_events=[],
        acb_final=[],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )
    detail = build_detail_report(
        income_events=[],
        acb_final=[],
        dispositions=[d1],
        tax_year=2025,
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    # Superficial loss detail lives in the detail file
    assert "Superficial Losses" in detail
    assert "2,100.00" in detail
    assert "0.00" in detail
    # Summary still flags the row with ⚠ in the dispositions table
    assert "\u26a0 Superficial loss" in summary
    # Net adjusted gain/loss is 0 (SL fully denied)
    net_line = [line for line in summary.splitlines() if "Net capital gain" in line]
    assert any("0.00" in line for line in net_line)


# ---------------------------------------------------------------------------
# Test 3: Quebec Part 4 grouped by asset
# ---------------------------------------------------------------------------


def test_quebec_part4_grouped_by_asset() -> None:
    btc_d1 = _adj_disp(
        id=1,
        timestamp=datetime(2025, 3, 15, 14, 0, 0, tzinfo=_UTC),
        asset="BTC",
        units="0.1",
        proceeds="5000",
        acb="3000",
        fees="50",
        gain_loss="1950",
        adjusted_gain_loss="1950",
    )
    btc_d2 = _adj_disp(
        id=2,
        timestamp=datetime(2025, 6, 10, 12, 0, 0, tzinfo=_UTC),
        asset="BTC",
        units="0.08",
        proceeds="3000",
        acb="2000",
        fees="30",
        gain_loss="970",
        adjusted_gain_loss="970",
    )
    eth_d3 = _adj_disp(
        id=3,
        timestamp=datetime(2025, 9, 5, 16, 0, 0, tzinfo=_UTC),
        asset="ETH",
        units="0.5",
        proceeds="1000",
        acb="800",
        fees="20",
        gain_loss="180",
        adjusted_gain_loss="180",
    )

    report = build_report(
        dispositions=[btc_d1, btc_d2, eth_d3],
        income_events=[],
        acb_final=[],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    assert "TP-21.4.39-V \u2014 Part 4" in report
    # BTC proceeds sum = 5000 + 3000 = 8000
    assert "8,000.00" in report
    # ETH proceeds = 1000
    assert "1,000.00" in report
    # Both assets appear as rows in Part 4
    part4_start = report.index("TP-21.4.39-V \u2014 Part 4")
    part4_section = report[part4_start : part4_start + 1000]
    assert "BTC" in part4_section
    assert "ETH" in part4_section


# ---------------------------------------------------------------------------
# Test 4: income section
# ---------------------------------------------------------------------------


def test_income_section() -> None:
    ev = _income(
        id=1,
        timestamp=datetime(2025, 2, 14, 8, 0, 0, tzinfo=_UTC),
        units="0.001",
        fmv_cad="75.00",
    )

    report = build_report(
        dispositions=[],
        income_events=[ev],
        acb_final=[],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    assert "TP-21.4.39-V \u2014 Part 6" in report
    assert "75.00" in report
    assert "staking" in report
    assert "Total cryptoasset income: 75.00 CAD" in report


# ---------------------------------------------------------------------------
# Test 5: empty report
# ---------------------------------------------------------------------------


def test_empty_report() -> None:
    report = build_report(
        dispositions=[],
        income_events=[],
        acb_final=[],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    assert isinstance(report, str)
    assert len(report) > 0
    assert "No dispositions recorded for 2025" in report
    assert "No open positions at year end" in report
    # Taxable gains line shows 0.00
    assert "0.00" in report


# ---------------------------------------------------------------------------
# Test 6: year-end holdings section
# ---------------------------------------------------------------------------


def test_year_end_holdings_section() -> None:
    state = ACBState(
        asset="BTC",
        total_units=Decimal("0.5"),
        total_acb_cad=Decimal("15000.00"),
        acb_per_unit_cad=Decimal("30000.00"),
    )

    report = build_report(
        dispositions=[],
        income_events=[],
        acb_final=[state],
        acquisitions=[],
        tax_year=2025,
        inclusion_rate=Decimal("0.50"),
        batch_name="test2025",
        sirop_version="0.1.0",
    )

    assert "TP-21.4.39-V \u2014 Part 5" in report
    assert "0.50000000" in report
    assert "15,000.00" in report
    assert "30,000.00" in report
