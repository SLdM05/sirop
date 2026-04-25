"""Tests for reward discount vs income treatment in the transfer matcher.

Covers _classify_income() for TransactionType.REWARD_SHAKE,
TransactionType.REWARD_SHAKESQUAD, TransactionType.REWARD_CASHBACK,
and TransactionType.INTEREST with both "discount" and "income" treatment.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sirop.models.enums import TransactionType
from sirop.models.transaction import Transaction
from sirop.transfer_match.matcher import match_transfers

_BASE_TS = datetime(2025, 2, 1, 12, 0, 0, tzinfo=UTC)

_FMV = Decimal("75000")  # fake BTC price in CAD


def _reward_tx(tx_type: TransactionType, amount: str = "0.00001") -> Transaction:
    return Transaction(
        id=1,
        source="shakepay",
        timestamp=_BASE_TS,
        tx_type=tx_type,
        asset="BTC",
        amount=Decimal(amount),
        cad_value=Decimal(amount) * _FMV,
        fee_cad=Decimal("0"),
        fee_crypto=Decimal("0"),
        txid=None,
        is_transfer=False,
        counterpart_id=None,
        notes="",
        wallet_id=None,
    )


# ---------------------------------------------------------------------------
# Discount treatment
# ---------------------------------------------------------------------------


def test_discount_reward_shake_no_income_event() -> None:
    """reward_shake with discount treatment produces no IncomeEvent."""
    tx = _reward_tx(TransactionType.REWARD_SHAKE)
    _events, income_evts, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shake": "discount"},
    )
    assert income_evts == [], "discount treatment must not create an IncomeEvent"


def test_discount_reward_shake_zero_acb() -> None:
    """reward_shake with discount treatment produces a ClassifiedEvent with cad_cost=0."""
    tx = _reward_tx(TransactionType.REWARD_SHAKE)
    events, _, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shake": "discount"},
    )
    taxable = [e for e in events if e.is_taxable]
    assert len(taxable) == 1
    assert taxable[0].event_type == "income"
    assert taxable[0].cad_cost == Decimal("0")


def test_discount_reward_cashback_no_income_event() -> None:
    """reward_cashback with discount treatment produces no IncomeEvent."""
    tx = _reward_tx(TransactionType.REWARD_CASHBACK)
    _events, income_evts, _ = match_transfers(
        [tx],
        reward_treatment={"reward_cashback": "discount"},
    )
    assert income_evts == []


def test_discount_reward_shakesquad_no_income_event() -> None:
    """reward_shakesquad with discount treatment produces no IncomeEvent."""
    tx = _reward_tx(TransactionType.REWARD_SHAKESQUAD)
    _events, income_evts, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shakesquad": "discount"},
    )
    assert income_evts == []


def test_discount_reward_shakesquad_zero_acb() -> None:
    """reward_shakesquad with discount treatment produces a ClassifiedEvent with cad_cost=0."""
    tx = _reward_tx(TransactionType.REWARD_SHAKESQUAD)
    events, _, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shakesquad": "discount"},
    )
    taxable = [e for e in events if e.is_taxable]
    assert len(taxable) == 1
    assert taxable[0].cad_cost == Decimal("0")


def test_interest_income_creates_income_event() -> None:
    """interest with income treatment creates an IncomeEvent at FMV."""
    tx = _reward_tx(TransactionType.INTEREST, amount="0.0000001")
    _events, income_evts, _ = match_transfers(
        [tx],
        reward_treatment={"interest": "income"},
    )
    assert len(income_evts) == 1
    assert income_evts[0].fmv_cad == tx.cad_value


def test_interest_income_fmv_acb() -> None:
    """interest with income treatment uses FMV as ACB."""
    tx = _reward_tx(TransactionType.INTEREST, amount="0.0000001")
    events, _, _ = match_transfers(
        [tx],
        reward_treatment={"interest": "income"},
    )
    taxable = [e for e in events if e.is_taxable]
    assert len(taxable) == 1
    assert taxable[0].cad_cost == tx.cad_value


# ---------------------------------------------------------------------------
# Income treatment
# ---------------------------------------------------------------------------


def test_income_reward_shake_creates_income_event() -> None:
    """reward_shake with income treatment creates an IncomeEvent at FMV."""
    tx = _reward_tx(TransactionType.REWARD_SHAKE, amount="0.00001")
    _events, income_evts, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shake": "income"},
    )
    assert len(income_evts) == 1
    assert income_evts[0].fmv_cad == tx.cad_value


def test_income_reward_shake_fmv_acb() -> None:
    """reward_shake with income treatment uses FMV as ACB."""
    tx = _reward_tx(TransactionType.REWARD_SHAKE, amount="0.00001")
    events, _, _ = match_transfers(
        [tx],
        reward_treatment={"reward_shake": "income"},
    )
    taxable = [e for e in events if e.is_taxable]
    assert len(taxable) == 1
    assert taxable[0].cad_cost == tx.cad_value


# ---------------------------------------------------------------------------
# Default fallback: unknown reward types default to income treatment
# ---------------------------------------------------------------------------


def test_unknown_reward_type_defaults_to_income() -> None:
    """A reward subtype not in reward_treatment config defaults to income treatment."""
    tx = _reward_tx(TransactionType.REWARD_SHAKE)
    # Empty reward_treatment — no override specified
    events, income_evts, _ = match_transfers([tx], reward_treatment={})
    assert len(income_evts) == 1, "default treatment is income (conservative fallback)"
    taxable = [e for e in events if e.is_taxable]
    assert taxable[0].cad_cost == tx.cad_value
