from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ACBState:
    """Snapshot of per-asset ACB pool after a transaction."""

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
    """A taxable disposition with computed gain/loss."""

    transaction_id: str
    timestamp: datetime
    asset: str
    amount: Decimal
    proceeds_cad: Decimal
    acb_of_disposed_cad: Decimal
    selling_fees_cad: Decimal
    gain_loss_cad: Decimal
    acb_state_before: ACBState
    acb_state_after: ACBState
    is_superficial_loss: bool
    denied_loss_cad: Decimal
    allowable_loss_cad: Decimal
    adjusted_gain_loss_cad: Decimal
    disposition_type: str
    year_acquired: str
