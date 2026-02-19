import dataclasses
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from sirop.models.disposition import ACBState, Disposition
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
        id="fake-id-001",
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
    assert tx.tx_type == TransactionType.BUY
    assert tx.asset == "BTC"


def test_transaction_is_frozen() -> None:
    tx = Transaction(
        id="fake-id-002",
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
# Disposition
# ---------------------------------------------------------------------------


def _make_disposition(transaction_id: str) -> Disposition:
    zero = ACBState.zero("BTC")
    return Disposition(
        transaction_id=transaction_id,
        timestamp=_TS,
        asset="BTC",
        amount=Decimal("0.1"),
        proceeds_cad=Decimal("7000.00"),
        acb_of_disposed_cad=Decimal("5000.00"),
        selling_fees_cad=Decimal("10.00"),
        gain_loss_cad=Decimal("1990.00"),
        acb_state_before=zero,
        acb_state_after=zero,
        is_superficial_loss=False,
        denied_loss_cad=Decimal("0"),
        allowable_loss_cad=Decimal("0"),
        adjusted_gain_loss_cad=Decimal("1990.00"),
        disposition_type="sell",
        year_acquired="2024",
    )


def test_disposition_instantiation() -> None:
    disp = _make_disposition("fake-tx-001")
    assert disp.asset == "BTC"
    assert disp.gain_loss_cad == Decimal("1990.00")
    assert not disp.is_superficial_loss


def test_disposition_is_frozen() -> None:
    disp = _make_disposition("fake-tx-002")
    with pytest.raises(dataclasses.FrozenInstanceError):
        disp.asset = "ETH"  # type: ignore[misc]
