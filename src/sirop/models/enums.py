from enum import StrEnum


class TransactionType(StrEnum):
    BUY = "buy"
    SELL = "sell"
    TRADE = "trade"
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    INCOME = "income"
    REWARD_SHAKE = "reward_shake"  # ShakingSats loyalty rewards
    REWARD_SHAKESQUAD = "reward_shakesquad"  # Shakesquads referral/loyalty rewards
    REWARD_CASHBACK = "reward_cashback"  # Shakepay Card cashback
    INTEREST = "interest"  # Savings/lending interest income
    SPEND = "spend"
    FEE = "fee"
    FIAT_DEPOSIT = "fiat_deposit"
    FIAT_WITHDRAWAL = "fiat_withdrawal"
    OTHER = "other"
