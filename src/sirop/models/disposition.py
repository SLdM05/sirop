from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ACBState:
    """Snapshot of per-asset ACB pool at a point in time."""

    asset: str
    total_units: Decimal
    total_acb_cad: Decimal
    acb_per_unit_cad: Decimal

    @staticmethod
    def zero(asset: str) -> "ACBState":
        return ACBState(
            asset=asset,
            total_units=Decimal("0"),
            total_acb_cad=Decimal("0"),
            acb_per_unit_cad=Decimal("0"),
        )


@dataclass(frozen=True)
class Disposition:
    """Output of the ACB engine (boil stage) for a single disposal event.

    Contains the raw gain/loss before superficial loss adjustment.
    The superficial loss stage reads this and writes AdjustedDisposition.
    """

    id: int  # matches dispositions.id (set by repository after insert)
    event_id: int  # FK → classified_events.id
    timestamp: datetime
    asset: str
    units: Decimal
    proceeds_cad: Decimal
    acb_of_disposed_cad: Decimal
    selling_fees_cad: Decimal  # Schedule 3 Column 4 (outlays and expenses)
    gain_loss_cad: Decimal  # proceeds - acb_of_disposed - selling_fees
    disposition_type: str  # 'sell' | 'trade' | 'spend' | 'fee_disposal'
    year_acquired: str  # e.g. "2024" or "Various" — Schedule 3 Column 1
    acb_state_before: ACBState  # pool state immediately before this disposal
    acb_state_after: ACBState  # pool state immediately after this disposal


@dataclass(frozen=True)
class AdjustedDisposition:
    """Output of the superficial loss stage for a single disposal event.

    Mirrors dispositions_adjusted in the DB. This is the authoritative
    source for Schedule 3, Schedule G, and TP-21.4.39-V Part 4 reporting.
    """

    id: int  # matches dispositions_adjusted.id
    disposition_id: int  # FK → dispositions.id
    timestamp: datetime
    asset: str
    units: Decimal
    proceeds_cad: Decimal
    acb_of_disposed_cad: Decimal
    selling_fees_cad: Decimal  # Schedule 3 Column 4
    gain_loss_cad: Decimal  # pre-adjustment gain/loss
    is_superficial_loss: bool
    superficial_loss_denied_cad: Decimal  # denied portion (added to repurchase ACB)
    allowable_loss_cad: Decimal  # loss that CAN be claimed (0 if not a loss)
    adjusted_gain_loss_cad: Decimal  # final number → Schedule 3 Column 5
    adjusted_acb_of_repurchase_cad: Decimal | None  # added to ACB of repurchased units
    disposition_type: str  # carried from Disposition
    year_acquired: str  # carried from Disposition


@dataclass(frozen=True)
class IncomeEvent:
    """A crypto income event: staking reward, airdrop, or mining receipt.

    Written by the transfer_match stage when an incoming transaction is
    classified as income (not a capital acquisition). Also appears in
    classified_events as an acquisition so the ACB engine can pick it up.

    Used by the report stage for TP-21.4.39-V Part 6 and Schedule G income rows.
    """

    id: int  # matches income_events.id
    vtx_id: int  # FK → verified_transactions.id
    timestamp: datetime
    asset: str  # e.g. "BTC"
    units: Decimal  # amount received
    income_type: str  # 'staking' | 'airdrop' | 'mining' | 'other'
    fmv_cad: Decimal  # FMV at receipt — this IS the taxable income amount
    source: str  # origin platform/wallet
