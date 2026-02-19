import dataclasses
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from sirop.models.disposition import (
    ACBState,
    AdjustedDisposition,
    Disposition,
    IncomeEvent,
)
from sirop.models.enums import TransactionType
from sirop.models.raw import RawTransaction
from sirop.models.transaction import Transaction

_TS = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# RawTransaction
# ---------------------------------------------------------------------------


def test_raw_transaction_instantiation() -> None:
    tx = RawTransaction(
        source="shakepay",
        timestamp=_TS,
        transaction_type="trade",
        asset="BTC",
        amount=Decimal("0.01"),
        amount_currency="BTC",
        fiat_value=Decimal("600.00"),
        fiat_currency="CAD",
        fee_amount=None,
        fee_currency=None,
        rate=Decimal("60000.00"),
        spot_rate=Decimal("60100.00"),
        txid="aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee5",
        raw_type="purchase/sale",
        raw_row={"Date": "2025-03-15T12:00:00+00:00"},
    )
    assert tx.source == "shakepay"
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.01")
    assert tx.amount_currency == "BTC"
    assert tx.fiat_currency == "CAD"


def test_raw_transaction_is_frozen() -> None:
    tx = RawTransaction(
        source="shakepay",
        timestamp=_TS,
        transaction_type="trade",
        asset="BTC",
        amount=Decimal("0.01"),
        amount_currency="BTC",
        fiat_value=None,
        fiat_currency=None,
        fee_amount=None,
        fee_currency=None,
        rate=None,
        spot_rate=None,
        txid=None,
        raw_type="purchase/sale",
        raw_row={},
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        tx.source = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


def test_transaction_instantiation() -> None:
    tx = Transaction(
        id=1,
        source="shakepay",
        timestamp=_TS,
        tx_type=TransactionType.BUY,
        asset="BTC",
        amount=Decimal("0.01"),
        cad_value=Decimal("600.00"),
        fee_cad=Decimal("0.00"),
        fee_crypto=Decimal("0.00"),
        txid=None,
        is_transfer=False,
        counterpart_id=None,
        notes="",
    )
    assert tx.id == 1
    assert tx.tx_type == TransactionType.BUY
    assert tx.asset == "BTC"
    assert not tx.is_transfer
    assert tx.counterpart_id is None


def test_transaction_transfer_link() -> None:
    """counterpart_id links two legs of a wallet-to-wallet transfer."""
    deposit_id = 11
    withdrawal = Transaction(
        id=10,
        source="shakepay",
        timestamp=_TS,
        tx_type=TransactionType.TRANSFER_OUT,
        asset="BTC",
        amount=Decimal("0.05"),
        cad_value=Decimal("3000.00"),
        fee_cad=Decimal("0.00"),
        fee_crypto=Decimal("0.00001"),
        txid="aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee5",
        is_transfer=True,
        counterpart_id=deposit_id,
        notes="",
    )
    assert withdrawal.is_transfer
    assert withdrawal.counterpart_id == deposit_id


def test_transaction_is_frozen() -> None:
    tx = Transaction(
        id=2,
        source="shakepay",
        timestamp=_TS,
        tx_type=TransactionType.SELL,
        asset="BTC",
        amount=Decimal("0.005"),
        cad_value=Decimal("300.00"),
        fee_cad=Decimal("1.50"),
        fee_crypto=Decimal("0.00"),
        txid=None,
        is_transfer=False,
        counterpart_id=None,
        notes="",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        tx.asset = "ETH"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ACBState
# ---------------------------------------------------------------------------


def test_acb_state_instantiation() -> None:
    state = ACBState(
        asset="BTC",
        total_units=Decimal("0.5"),
        total_acb_cad=Decimal("15000.00"),
        acb_per_unit_cad=Decimal("30000.00"),
    )
    assert state.asset == "BTC"
    assert state.acb_per_unit_cad == Decimal("30000.00")


def test_acb_state_zero() -> None:
    state = ACBState.zero("BTC")
    assert state.asset == "BTC"
    assert state.total_units == Decimal("0")
    assert state.total_acb_cad == Decimal("0")
    assert state.acb_per_unit_cad == Decimal("0")


def test_acb_state_is_frozen() -> None:
    state = ACBState.zero("BTC")
    with pytest.raises(dataclasses.FrozenInstanceError):
        state.total_units = Decimal("1")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Disposition  (ACB engine output — no superficial loss fields)
# ---------------------------------------------------------------------------


def _make_disposition(event_id: int = 1, disp_id: int = 1) -> Disposition:
    before = ACBState(
        asset="BTC",
        total_units=Decimal("0.5"),
        total_acb_cad=Decimal("25000.00"),
        acb_per_unit_cad=Decimal("50000.00"),
    )
    after = ACBState(
        asset="BTC",
        total_units=Decimal("0.4"),
        total_acb_cad=Decimal("20000.00"),
        acb_per_unit_cad=Decimal("50000.00"),
    )
    return Disposition(
        id=disp_id,
        event_id=event_id,
        timestamp=_TS,
        asset="BTC",
        units=Decimal("0.1"),
        proceeds_cad=Decimal("7000.00"),
        acb_of_disposed_cad=Decimal("5000.00"),
        selling_fees_cad=Decimal("10.00"),
        gain_loss_cad=Decimal("1990.00"),
        disposition_type="sell",
        year_acquired="2024",
        acb_state_before=before,
        acb_state_after=after,
    )


def test_disposition_instantiation() -> None:
    disp = _make_disposition()
    assert disp.asset == "BTC"
    assert disp.gain_loss_cad == Decimal("1990.00")
    assert disp.selling_fees_cad == Decimal("10.00")
    assert disp.disposition_type == "sell"
    assert disp.year_acquired == "2024"
    assert disp.acb_state_before.total_units == Decimal("0.5")
    assert disp.acb_state_after.total_units == Decimal("0.4")


def test_disposition_is_frozen() -> None:
    disp = _make_disposition(disp_id=2)
    with pytest.raises(dataclasses.FrozenInstanceError):
        disp.asset = "ETH"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# AdjustedDisposition  (superficial loss stage output)
# ---------------------------------------------------------------------------


def _make_adjusted(is_superficial: bool = False) -> AdjustedDisposition:
    denied = Decimal("1990.00") if is_superficial else Decimal("0")
    allowable = Decimal("0") if is_superficial else Decimal("1990.00")
    adjusted = Decimal("0") if is_superficial else Decimal("1990.00")
    return AdjustedDisposition(
        id=1,
        disposition_id=1,
        timestamp=_TS,
        asset="BTC",
        units=Decimal("0.1"),
        proceeds_cad=Decimal("7000.00"),
        acb_of_disposed_cad=Decimal("5000.00"),
        selling_fees_cad=Decimal("10.00"),
        gain_loss_cad=Decimal("1990.00"),
        is_superficial_loss=is_superficial,
        superficial_loss_denied_cad=denied,
        allowable_loss_cad=allowable,
        adjusted_gain_loss_cad=adjusted,
        adjusted_acb_of_repurchase_cad=denied if is_superficial else None,
        disposition_type="sell",
        year_acquired="2024",
    )


def test_adjusted_disposition_no_superficial_loss() -> None:
    adj = _make_adjusted(is_superficial=False)
    assert not adj.is_superficial_loss
    assert adj.superficial_loss_denied_cad == Decimal("0")
    assert adj.adjusted_gain_loss_cad == Decimal("1990.00")
    assert adj.adjusted_acb_of_repurchase_cad is None


def test_adjusted_disposition_superficial_loss() -> None:
    adj = _make_adjusted(is_superficial=True)
    assert adj.is_superficial_loss
    assert adj.superficial_loss_denied_cad == Decimal("1990.00")
    assert adj.adjusted_gain_loss_cad == Decimal("0")
    assert adj.adjusted_acb_of_repurchase_cad == Decimal("1990.00")


def test_adjusted_disposition_is_frozen() -> None:
    adj = _make_adjusted()
    with pytest.raises(dataclasses.FrozenInstanceError):
        adj.asset = "ETH"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# IncomeEvent
# ---------------------------------------------------------------------------


def test_income_event_instantiation() -> None:
    event = IncomeEvent(
        id=1,
        vtx_id=42,
        timestamp=_TS,
        asset="BTC",
        units=Decimal("0.001"),
        income_type="staking",
        fmv_cad=Decimal("65.00"),
        source="shakepay",
    )
    assert event.asset == "BTC"
    assert event.income_type == "staking"
    assert event.fmv_cad == Decimal("65.00")
    assert event.units == Decimal("0.001")


def test_income_event_is_frozen() -> None:
    event = IncomeEvent(
        id=2,
        vtx_id=43,
        timestamp=_TS,
        asset="BTC",
        units=Decimal("0.0005"),
        income_type="airdrop",
        fmv_cad=Decimal("0.00"),
        source="sparrow",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.income_type = "staking"  # type: ignore[misc]
