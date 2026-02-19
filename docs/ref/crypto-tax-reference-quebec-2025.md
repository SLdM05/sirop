# Crypto tax tool spec for Quebec 2025

**The 2025 tax year uses a straightforward 50% capital gains inclusion rate with no threshold changes**, after the proposed increase to 66.67% was cancelled in March 2025. For a Quebec personal investor, filing requires both the federal Schedule 3 and Quebec's mandatory TP-21.4.39-V cryptoasset return — a Quebec-specific form with per-day penalties for non-compliance that has no federal equivalent. The CRA requires weighted-average Adjusted Cost Base (ACB) calculations for all crypto, treats wallet-to-wallet transfers as non-taxable, and applies the superficial loss rule to digital assets. What follows is a complete technical specification covering every rule, formula, data format, and form reference needed to build the Python calculation tool.

---

## 1. How the CRA classifies crypto and what triggers tax

The CRA treats cryptocurrency as a **commodity and capital property** — not legal tender. For a personal investor (buy-and-hold, not frequent trading), dispositions generate capital gains or losses. The CRA determines capital vs. business treatment on a case-by-case basis using factors from Interpretation Bulletin IT-479R: frequency of transactions, holding period, knowledge of markets, time spent, financing used, and advertising activity. A long-term holder of BTC on Shakepay/NDAX clearly falls on the capital property side.

### Taxable events (dispositions)

Every event below requires capital gain/loss calculation in CAD at the transaction date:

- **Selling crypto for CAD** — proceeds minus ACB minus fees equals gain/loss
- **Crypto-to-crypto trades** — treated as simultaneous disposition of one asset and acquisition of another, both at fair market value (FMV) in CAD
- **Using crypto to pay for goods or services** — barter transaction, deemed disposition at FMV
- **Gifting crypto** — deemed disposition at FMV for the giver

### Non-taxable events (no disposition)

- **Buying crypto with CAD** — acquisition only, establishes ACB
- **Transferring between own wallets** (e.g., Shakepay → Sparrow cold wallet) — not a disposition; ACB carries forward unchanged
- **Holding unrealized gains** — no tax until disposition
- **Depositing crypto for staking** — CRA confirmed in 2025 this is not a disposition if beneficial ownership is retained

### Events taxed as income (not capital gains)

- **Staking rewards** — taxable as income at FMV when received; subsequent sale triggers a separate capital gain/loss from the income-established ACB
- **Airdrops** — likely income at FMV on receipt (CRA hasn't issued definitive guidance; conservative approach is to report as income; if FMV is zero at receipt, ACB is effectively $0)
- **Mining rewards** — typically business income if commercial, otherwise "other income"

---

## 2. Capital gains inclusion rate: confirmed at 50% for 2025

The federal government proposed increasing the inclusion rate from **50% to 66.67%** for individual capital gains exceeding $250,000 annually (Budget 2024, originally effective June 25, 2024). On January 31, 2025, Finance Minister Dominic LeBlanc deferred the effective date to January 1, 2026. On **March 21, 2025, Prime Minister Mark Carney cancelled the increase entirely**. The November 2025 federal budget confirmed the cancellation, and Quebec's Fall 2025 update explicitly aligned with this decision.

For the Python tool, the constant is simple:

```python
CAPITAL_GAINS_INCLUSION_RATE = 0.50  # All capital gains for individuals, no threshold
```

One caveat: the **Alternative Minimum Tax (AMT)** reforms from January 1, 2024, remain in effect. AMT uses a **100% inclusion rate** on capital gains (up from 80%) with a 20.5% federal AMT rate. Investors with very large gains should be flagged for potential AMT exposure, though this is an edge case for most personal investors.

---

## 3. ACB calculation: the weighted-average method

The CRA mandates the **Adjusted Cost Base (ACB)** method — a running weighted-average cost across all identical units of a cryptocurrency. **FIFO and LIFO are not permitted** for identical properties in Canada. Each cryptocurrency (BTC, ETH, etc.) maintains its own separate ACB pool.

### Core formulas

**On acquisition (buy):**
```
new_total_ACB = previous_total_ACB + purchase_cost_CAD + purchase_fees_CAD
new_total_units = previous_total_units + units_acquired
ACB_per_unit = new_total_ACB / new_total_units
```

**On disposition (sell):**
```
cost_of_units_sold = ACB_per_unit × units_sold
capital_gain_loss = proceeds_CAD - cost_of_units_sold - selling_fees_CAD
remaining_total_ACB = previous_total_ACB - cost_of_units_sold
remaining_units = previous_total_units - units_sold
```

### Worked example

| Step | Action | Units | Cost/Proceeds (CAD) | Fees | Running ACB | Total Units | ACB/Unit |
|------|--------|-------|---------------------|------|-------------|-------------|----------|
| 1 | Buy | 0.5 BTC | $25,000 | $50 | $25,050 | 0.5 | $50,100 |
| 2 | Buy | 0.3 BTC | $18,000 | $36 | $43,086 | 0.8 | $53,857.50 |
| 3 | Sell | 0.4 BTC | $28,000 | $40 | $21,543 | 0.4 | $53,857.50 |

**Step 3 gain:** $28,000 − ($53,857.50 × 0.4) − $40 = **$6,417.00**. Taxable capital gain at 50% = **$3,208.50**.

### Fee treatment rules

- **Exchange trading fees on purchase**: added to ACB (increases cost basis)
- **Exchange trading fees on sale**: subtracted from proceeds (treated as "outlays and expenses" on Schedule 3, Column 4)
- **Network/blockchain fees for transfers between own wallets**: two defensible approaches — (a) treat the fee portion as a micro-disposition of crypto, calculating a tiny gain/loss, or (b) add the CAD value of the fee to the ACB of the transferred asset. The tool should default to micro-disposition for strictest compliance, with a configuration flag for the alternative.
- **Exchange withdrawal fees** charged in crypto: same treatment as network fees

### Identifying wallet-to-wallet transfers

Transfers between own wallets are **not dispositions**. The algorithm to detect them:

1. For each withdrawal from an exchange, search for a matching deposit in another wallet/exchange
2. Match on: same asset type, amount within tolerance (withdrawal amount minus typical network fee), timestamps within a reasonable window (minutes to a few hours depending on blockchain)
3. If a matching blockchain transaction hash (txid) exists on both sides, that's a definitive match
4. ACB carries forward unchanged; only the network fee needs treatment

---

## 4. Superficial loss rule: the 61-day window

The superficial loss rule (Section 54 of the Income Tax Act) **applies to cryptocurrency**. The CRA treats crypto as property, and the rule applies to all property dispositions at a loss.

### Trigger conditions (all must be met)

1. You sell crypto at a **loss**
2. You (or an affiliated person — spouse, common-law partner, or controlled corporation) acquire the **same cryptocurrency** during the period from **30 days before to 30 days after** the sale (61-day window total)
3. At the end of the 30th day after the sale, you or the affiliated person **still hold** the identical property

### When triggered

The capital loss is **denied** and added to the ACB of the repurchased units, effectively deferring (not destroying) the loss.

```python
# Adjustment when superficial loss applies
denied_loss = abs(capital_loss)
new_ACB_of_repurchased = cost_of_repurchased_units + denied_loss
```

### Partial superficial loss (fewer units reacquired than sold)

```python
superficial_portion = min(units_sold, units_reacquired, units_held_at_end_of_window)
denied_loss = total_loss * (superficial_portion / units_sold)
allowable_loss = total_loss - denied_loss
```

**"Identical property" for crypto** means the same token regardless of exchange or wallet. BTC on Shakepay and BTC on NDAX are identical. BTC and ETH are not identical. The rule applies across all wallets and affiliated persons.

---

## 5. Quebec-specific rules and the mandatory TP-21.4.39-V

Quebec **generally follows identical substantive rules** to the CRA for crypto taxation: same ACB method, same 50% inclusion rate for 2025, same capital loss carry-back (3 years) and carry-forward (indefinite). The key difference is that Quebec residents file **two separate returns** — federal T1 to CRA and provincial TP-1 to Revenu Québec — and Quebec imposes a **mandatory cryptoasset disclosure form**.

### TP-21.4.39-V: Quebec's cryptoasset return

This form is **mandatory for anyone who owned, received, disposed of, or used cryptoassets** during the tax year — even if you merely held crypto with no transactions. It was introduced for the 2024 tax year onward.

The form requires reporting **by type of cryptoasset** (separate entries for BTC, ETH, etc.) and includes:

- **Part 1**: Identification
- **Part 2**: Information on cryptoasset holdings (balances)
- **Part 3**: Information on platforms/wallets used
- **Part 4**: Capital gains or losses per crypto type (units disposed, proceeds, ACB, expenses, gain/loss)
- **Part 5**: Business income from cryptoassets (not applicable for personal investor)
- **Part 6**: Property income from cryptoassets (staking/lending income)

**Penalties for non-filing are steep**: **$10 per day** (maximum $2,500) after the filing due date, plus **$100 per omitted or erroneous item**. As of April 2025, taxpayers no longer need to enclose detailed transaction statements but must retain them for audit.

### Quebec forms and line numbers

| Form | Purpose | Key Line |
|------|---------|----------|
| **TP-1-V** | Quebec income tax return | Line 24 (crypto declaration checkbox), Line 139 (taxable capital gains), Line 290 (prior-year capital losses) |
| **Schedule G** | Capital gains and losses (equivalent to federal Schedule 3) | Part A for crypto dispositions → flows to Line 139 |
| **TP-21.4.39-V** | Cryptoasset return (mandatory) | Parts 4–6 for gains/income by crypto type |
| **TP-1012.A-V** | Carry-back of a loss | For applying capital losses to prior years |

### Quebec-specific tax impacts from crypto gains

Capital gains flow into Quebec net income (Line 275), which can **reduce or eliminate the Solidarity Tax Credit** (Schedule D) and **increase the RAMQ prescription drug insurance premium** (Schedule K, maximum ~$755/person for 2025). The **Health Services Fund (HSF)** contribution (Schedule F) also applies to capital gains at a rate up to approximately 1%. These are unique additional costs for Quebec residents.

### Combined top marginal rate on capital gains (2025)

Federal effective rate for Quebec residents (after 16.5% Quebec abatement): ~**13.78%** on the taxable portion. Quebec top rate on the taxable portion: **12.875%** (25.75% × 50%). Combined maximum effective rate on the actual capital gain: approximately **26.65%**, plus the minor HSF contribution.

---

## 6. T1135 foreign property reporting

Crypto held on **foreign exchanges** (Binance, Coinbase, Kraken US) likely qualifies as "specified foreign property" per CRA Technical Interpretation 2014-0561061E5. If the total cost amount (ACB, not market value) of all specified foreign property exceeds **$100,000 CAD at any time during the year**, Form T1135 must be filed.

**Canadian exchanges like Shakepay and NDAX generally do not trigger T1135.** The CRA stated in November 2023 guidance that crypto held through compliant Canadian Crypto Trading Platforms (CTPs) is "typically not considered as located outside Canada." Since both Shakepay and NDAX are registered Canadian entities, their holdings are domestic.

**Cold storage (Sparrow wallet)** is a grey area — CRA has not issued definitive guidance on the situs of self-custodied crypto. The position remains "under review" per a June 2022 technical interpretation. **Professional consensus**: file T1135 if in doubt, given penalties of $25/day (max $2,500) for late filing.

For this user's situation (Canadian exchanges + cold wallet, no foreign exchange), **T1135 is likely not required** unless they have other foreign property pushing them over $100K.

---

## 7. Data export formats from each platform

### Shakepay CSV

Shakepay exports 2–3 separate CSV files (crypto, CAD, and optionally USD transactions). The column headers are:

```
Transaction Type, Date, Amount Debited, Debit Currency, Amount Credited,
Credit Currency, Buy / Sell Rate, Direction, Spot Rate,
Source / Destination, Blockchain Transaction ID
```

**Key notes for the parser**: Shakepay has **no explicit fee column**. Fees are embedded in the spread between the Buy/Sell Rate and the Spot Rate. The implicit fee can be calculated as `(Buy/Sell Rate - Spot Rate) × amount` for buys. Date format uses ISO 8601 with a non-standard timezone offset (`+00` instead of `+00:00`). Transaction types include `fiat funding`, `purchase/sale`, `crypto cashout`, and `other`. Koinly supports direct CSV import of this format.

### NDAX CSV

NDAX uses the AlphaPoint APEX platform. Export via Reports → Create Report → CSV → Ledgers report type. Expected columns:

```
Transaction Number, Date/Time, Product (Currency Symbol), Type,
CR (Credit), DR (Debit), Balance, Transaction Type, Reference ID
```

NDAX has the **strongest Koinly integration** of any platform here: SSO direct connection, API sync, and CSV import are all supported. The Ledgers report type is recommended by tax software providers.

### Sparrow Wallet CSV

Sparrow added CSV export in version 1.8.1 (late 2023). Expected columns:

```
Date, Label, Amount (BTC), Value (Fiat), Transaction ID
```

**Limitations**: Bitcoin-only, approximate daily fiat values (not exact time-of-transaction), no explicit fee column for outgoing transactions, and no direct Koinly integration. The recommended approach is to import into Koinly via **xpub or address import** from the Bitcoin blockchain rather than CSV, which provides more accurate and complete data.

### Koinly export formats

Koinly offers the richest export options and is the recommended primary data source for the Python tool. Key exports:

- **Capital Gains Report CSV**: Date Acquired, Date Sold, Asset, Amount, Cost Basis, Proceeds, Gain/Loss, Holding Period
- **Transaction History CSV**: Date, Type, Sent Amount, Sent Currency, Received Amount, Received Currency, Fee Amount, Fee Currency, Net Worth Amount, Net Worth Currency, Gain/Loss, Label, Description, TxHash
- **Complete Tax Report PDF**: Summary of all gains, income, and holdings
- **Schedule 3 (Canada-specific)**: Pre-filled federal Schedule 3 format
- **End of Year Holdings CSV**: Per-asset balances, values, and ACBs

Koinly uses the CRA-required ACB method, accounts for the superficial loss rule, and has been assessed by Canadian accounting firm MNP. However, **Koinly does not generate Quebec-specific forms** (TP-21.4.39-V or Schedule G) — those must be produced by the Python tool or completed manually.

### Minimum required fields for the Python tool

```python
REQUIRED_FIELDS = {
    'date': 'Transaction timestamp (UTC or with timezone)',
    'type': 'buy | sell | trade | transfer | income | fee',
    'asset': 'Cryptocurrency symbol (BTC, ETH, etc.)',
    'amount': 'Quantity of crypto',
    'cad_value': 'Fair market value in CAD at transaction time',
    'fee_amount': 'Fee quantity (if any)',
    'fee_currency': 'Fee denomination',
    'txid': 'Blockchain transaction hash (for matching transfers)',
}
```

---

## 8. Schedule 3 and Schedule G output specification

### Federal Schedule 3 (Form 5000-S3)

Crypto dispositions go on **Line 10 of Part 3**: "Bonds, debentures, Treasury bills, promissory notes, **crypto-assets**, and other properties." Five columns per entry:

| Column | Field | Python Variable |
|--------|-------|-----------------|
| 1 | Year of acquisition | `year_acquired` (int or "Various") |
| 2 | Proceeds of disposition | `proceeds_cad` (Decimal) |
| 3 | Adjusted cost base | `acb_of_disposed_units_cad` (Decimal) |
| 4 | Outlays and expenses | `selling_fees_cad` (Decimal) |
| 5 | Gain (or loss) | `col2 - col3 - col4` (Decimal) |

The total from Line 10 flows into the Part 3 subtotal, then through Part 4 (net capital gains/losses) to **Part 5** which calculates the taxable amount. The final taxable capital gain goes to **Line 12700 of the T1 return**. Net capital losses can be carried back 3 years (Form T1A) or forward indefinitely (Line 25300).

For the 2025 tax year, Schedule 3 should revert to a **single-period format** (the 2024 version was split into two periods due to the proposed mid-year inclusion rate change).

### Quebec Schedule G

Quebec's Schedule G mirrors the federal structure. Crypto dispositions go in **Part A** (capital gains or losses on disposition of capital property). The columns match: description, year of acquisition, proceeds, ACB, outlays/expenses, gain/loss. The net result flows to **Line 139 of the TP-1**.

### Quebec TP-21.4.39-V output

The Python tool must also generate data for this form, broken down **per cryptocurrency type**:

```python
@dataclass
class TP2143Entry:
    crypto_type: str              # e.g., "Bitcoin (BTC)"
    units_disposed: Decimal
    proceeds_cad: Decimal
    acb_of_disposed_cad: Decimal
    outlays_expenses_cad: Decimal
    capital_gain_loss_cad: Decimal
    # Also needed for Part 2: year-end holdings
    units_held_year_end: Decimal
    acb_held_year_end: Decimal
```

---

## 9. Complete Python tool architecture

### Implementation constants

```python
from decimal import Decimal

# Tax parameters — 2025 tax year
CAPITAL_GAINS_INCLUSION_RATE = Decimal("0.50")
SUPERFICIAL_LOSS_WINDOW_DAYS = 30  # 30 before + sale day + 30 after = 61 total
LOSS_CARRY_BACK_YEARS = 3
RECORD_RETENTION_YEARS = 6
T1135_THRESHOLD_CAD = Decimal("100000")

# Bank of Canada API (free, no key required)
BOC_VALET_URL = "https://www.bankofcanada.ca/valet/observations/{series}/json"
BOC_USD_SERIES = "FXUSDCAD"

# Federal form references
SCHEDULE_3_CRYPTO_LINE = 10
T1_TAXABLE_GAINS_LINE = 12700
T1_PRIOR_LOSSES_LINE = 25300

# Quebec form references
TP1_CRYPTO_CHECKBOX_LINE = 24
TP1_TAXABLE_GAINS_LINE = 139
TP1_PRIOR_LOSSES_LINE = 290
QC_CRYPTO_FORM = "TP-21.4.39-V"
```

### Processing pipeline

The tool should follow this sequence:

1. **Import** — Parse CSVs from Shakepay, NDAX, Sparrow, and/or Koinly into a unified transaction format
2. **Normalize** — Convert all amounts to CAD using Bank of Canada daily rates, standardize timestamps to UTC
3. **Classify** — Tag each transaction as buy, sell, trade, transfer, income, or fee
4. **Match transfers** — Identify wallet-to-wallet transfers (same asset, matching amounts minus network fee, matching txids, timestamps within hours)
5. **Calculate ACB** — Process transactions chronologically, maintaining a per-asset ACB pool using the weighted-average method
6. **Detect superficial losses** — For every disposition at a loss, check the 61-day window for repurchases and holdings at end of window; deny and reallocate losses as required
7. **Generate reports** — Produce Schedule 3 entries, Schedule G entries, TP-21.4.39-V per-asset summaries, and a comprehensive capital gains summary

### Currency conversion implementation

```python
import requests
from datetime import date, timedelta
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_boc_rate(currency: str, tx_date: date) -> Decimal:
    """Fetch Bank of Canada daily average rate with weekend fallback."""
    series = f"FX{currency}CAD"
    for offset in range(4):  # handle up to 3-day weekends
        query_date = tx_date - timedelta(days=offset)
        url = BOC_VALET_URL.format(series=series)
        resp = requests.get(url, params={
            "start_date": str(query_date),
            "end_date": str(query_date)
        })
        data = resp.json()
        if data.get("observations"):
            return Decimal(data["observations"][0][series]["v"])
    raise ValueError(f"No BoC rate for {currency} near {tx_date}")
```

### Superficial loss detection algorithm

```python
from datetime import timedelta

def check_superficial_loss(sale, all_transactions, holdings):
    """Returns (is_superficial, denied_amount, allowable_loss)."""
    if sale.gain_loss >= 0:
        return False, Decimal(0), sale.gain_loss

    window_start = sale.date - timedelta(days=30)
    window_end = sale.date + timedelta(days=30)

    # Units of same asset acquired in the 61-day window (excluding the sale itself)
    units_acquired_in_window = sum(
        t.amount for t in all_transactions
        if t.asset == sale.asset
        and t.type in ('buy', 'trade_in')
        and window_start <= t.date <= window_end
        and t is not sale
    )

    # Units still held at end of 30th day after sale
    units_held_at_end = holdings.get_balance(sale.asset, window_end)

    if units_acquired_in_window > 0 and units_held_at_end > 0:
        superficial_portion = min(sale.amount, units_acquired_in_window, units_held_at_end)
        denied = abs(sale.gain_loss) * (superficial_portion / sale.amount)
        allowable = abs(sale.gain_loss) - denied
        return True, denied, -allowable  # negative = allowable loss
    return False, Decimal(0), sale.gain_loss
```

---

## Conclusion: what makes this build tractable

The strongest implementation path is to **use Koinly's Capital Gains Report CSV as the primary data source**, since the user already has all transactions recorded there. Koinly handles the ACB calculations, superficial loss detection, and CAD conversion — the Python tool's main value-add is generating the **Quebec-specific outputs** (TP-21.4.39-V data and Schedule G formatting) that Koinly does not produce, plus providing an independent verification of Koinly's calculations.

Three implementation priorities stand out. First, the TP-21.4.39-V generator is the highest-value component since Koinly and most tax software ignore this mandatory Quebec form entirely, and penalties for non-filing are aggressive. Second, a transfer-matching module is essential because Shakepay-to-Sparrow transfers will appear as withdrawals and deposits across different CSVs and must not be treated as dispositions. Third, a Bank of Canada rate lookup with caching will ensure all USD-denominated DeFi transactions (Phantom wallet) convert accurately.

The 50% inclusion rate, single-pool ACB method, and straightforward Schedule 3 Line 10 reporting make the federal side mechanical. The Quebec side — with its per-asset-type reporting requirement on TP-21.4.39-V and the checkbox on Line 24 of the TP-1 — is where this tool provides the most value beyond what existing commercial software offers.