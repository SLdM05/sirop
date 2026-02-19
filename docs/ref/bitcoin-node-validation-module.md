# Bitcoin Node Verification Module

## Purpose

This module uses a local Bitcoin full node to verify, enrich, and audit on-chain transaction data used in crypto tax calculations. It serves as the authoritative source of truth for all BTC transactions, providing exact confirmation timestamps, precise fee amounts, and definitive transfer matching — data that exchange CSVs and wallet exports may report inaccurately or incompletely.

This module is called **after** the import/normalize pipeline steps and **before** ACB calculation. Every transaction with a `txid` is verified against the node. Discrepancies are flagged and the on-chain values are used as the canonical source.

---

## API Options

Two interfaces are supported. The tool should prefer whichever is available, with Bitcoin Core RPC as the primary and Mempool API as the fallback.

### Option A: Bitcoin Core RPC (direct, fastest, fully private)

Requires `bitcoin-cli` or JSON-RPC access to a local `bitcoind` instance.

```
RPC endpoint: http://127.0.0.1:8332
Auth: rpcuser/rpcpassword from bitcoin.conf (or cookie auth)
```

### Option B: Mempool API (REST, no auth, works with any Mempool instance)

If running a self-hosted Mempool instance backed by the local node:

```
Base URL: http://localhost:3006/api  (default Mempool port)
```

If using the public instance (less private, rate-limited):

```
Base URL: https://mempool.space/api
```

**Recommendation**: Self-hosted Mempool backed by your own node gives you the REST API convenience with full privacy and no rate limits.

---

## Data Points Retrieved Per Transaction

For each `txid` found in imported transaction data, the module retrieves:

| Field | Source (RPC) | Source (Mempool API) | Tax Relevance |
|-------|-------------|---------------------|---------------|
| Confirmation timestamp | `getblock` → `time` (unix epoch) | `GET /tx/{txid}` → `status.block_time` | Authoritative date for FMV lookup and tax year assignment |
| Block height | `getrawtransaction` → `blockhash` → `getblock` → `height` | `GET /tx/{txid}` → `status.block_height` | Audit trail, ordering |
| Confirmation count | `getrawtransaction` → `confirmations` | `GET /tx/{txid}` → `status.confirmed` | Verify transaction is settled |
| Total input value (sats) | Sum of `vin[].prevout.value` (requires `txindex=1`) | `GET /tx/{txid}` → sum of `vin[].prevout.value` | Fee calculation |
| Total output value (sats) | Sum of `vout[].value` | `GET /tx/{txid}` → sum of `vout[].value` | Fee calculation, amount verification |
| Fee (sats) | `inputs - outputs` (computed) | `GET /tx/{txid}` → `fee` (provided directly) | Network fee for ACB adjustment or micro-disposition |
| Fee rate (sat/vB) | Computed from fee / vsize | `GET /tx/{txid}` → `fee` / `weight * 4` | Informational / sanity check |
| Input addresses | Decoded from `vin[].prevout.scriptPubKey` | `GET /tx/{txid}` → `vin[].prevout.scriptpubkey_address` | Transfer matching (identify sender) |
| Output addresses | Decoded from `vout[].scriptPubKey` | `GET /tx/{txid}` → `vout[].scriptpubkey_address` | Transfer matching (identify recipient) |
| Transaction size/weight | `getrawtransaction` → `vsize`, `weight` | `GET /tx/{txid}` → `weight` | Informational |

---

## API Call Reference

### Bitcoin Core RPC

```python
import requests
from decimal import Decimal

class BitcoinRPC:
    def __init__(self, url="http://127.0.0.1:8332", user="", password=""):
        self.url = url
        self.auth = (user, password)
        self._id = 0

    def _call(self, method: str, params: list = None) -> dict:
        self._id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._id,
            "method": method,
            "params": params or []
        }
        resp = requests.post(self.url, json=payload, auth=self.auth)
        result = resp.json()
        if result.get("error"):
            raise Exception(f"RPC error: {result['error']}")
        return result["result"]

    def get_transaction(self, txid: str) -> dict:
        """Get decoded transaction. Requires txindex=1 in bitcoin.conf."""
        return self._call("getrawtransaction", [txid, True])

    def get_block(self, blockhash: str) -> dict:
        """Get block header and metadata."""
        return self._call("getblock", [blockhash])

    def get_block_at_height(self, height: int) -> dict:
        """Get block by height."""
        blockhash = self._call("getblockhash", [height])
        return self.get_block(blockhash)
```

**Key RPC methods used:**

| Method | Parameters | Returns |
|--------|-----------|---------|
| `getrawtransaction` | `txid`, `verbose=true` | Full decoded tx with `blockhash`, `confirmations`, `vin`, `vout` |
| `getblock` | `blockhash` | Block header with `time`, `height`, `nTx` |
| `getblockhash` | `height` | Block hash at given height |
| `gettxout` | `txid`, `vout_index` | UTXO details (for unspent outputs) |

**Required bitcoin.conf settings:**

```
server=1
txindex=1          # Required to look up arbitrary transactions
rpcuser=yourusername
rpcpassword=yourpassword
```

`txindex=1` is essential — without it, `getrawtransaction` only works for transactions in the mempool or with outputs in the UTXO set. If the node was started without `txindex`, it needs a full reindex: `bitcoind -reindex`.

### Mempool REST API

```python
import requests
from decimal import Decimal
from datetime import datetime, timezone

class MempoolAPI:
    def __init__(self, base_url="http://localhost:3006/api"):
        self.base_url = base_url.rstrip("/")

    def get_transaction(self, txid: str) -> dict:
        """Fetch full transaction details."""
        resp = requests.get(f"{self.base_url}/tx/{txid}")
        resp.raise_for_status()
        return resp.json()

    def get_tx_outspends(self, txid: str) -> list:
        """Check which outputs have been spent (useful for UTXO tracking)."""
        resp = requests.get(f"{self.base_url}/tx/{txid}/outspends")
        resp.raise_for_status()
        return resp.json()

    def get_address_txs(self, address: str) -> list:
        """Get transaction history for an address."""
        resp = requests.get(f"{self.base_url}/address/{address}/txs")
        resp.raise_for_status()
        return resp.json()

    def get_block_timestamp(self, height: int) -> int:
        """Get block timestamp by height."""
        resp = requests.get(f"{self.base_url}/block-height/{height}")
        resp.raise_for_status()
        block_hash = resp.text
        resp2 = requests.get(f"{self.base_url}/block/{block_hash}")
        resp2.raise_for_status()
        return resp2.json()["timestamp"]
```

**Key Mempool endpoints used:**

| Endpoint | Returns |
|----------|---------|
| `GET /tx/{txid}` | Full tx: `fee`, `status.block_time`, `status.block_height`, `vin[]`, `vout[]` |
| `GET /tx/{txid}/outspends` | Spending status of each output |
| `GET /address/{address}/txs` | All transactions for an address |
| `GET /block-height/{height}` | Block hash at height |
| `GET /block/{hash}` | Block details including `timestamp` |

**Rate limits (public mempool.space only):**
- 10 requests/second without Tor
- Self-hosted instance: no limits

---

## Verification Pipeline

### Step 1: Enrich each transaction with on-chain data

For every imported transaction that has a `txid`:

```python
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

SATS_PER_BTC = Decimal("100000000")

@dataclass
class OnChainData:
    txid: str
    block_height: int
    block_timestamp: datetime          # UTC, from block header
    fee_sats: int
    fee_btc: Decimal
    total_input_sats: int
    total_output_sats: int
    input_addresses: list[str]
    output_addresses: list[str]
    confirmations: int
    is_confirmed: bool

def enrich_transaction(txid: str, api) -> OnChainData:
    """Query node and return structured on-chain data."""
    tx = api.get_transaction(txid)

    # Mempool API provides fee directly; RPC requires computation
    if "fee" in tx:
        fee_sats = tx["fee"]
    else:
        total_in = sum(vin["prevout"]["value"] for vin in tx["vin"])
        total_out = sum(vout["value"] for vout in tx["vout"])
        fee_sats = total_in - total_out

    block_time = tx.get("status", {}).get("block_time") or tx.get("blocktime")

    return OnChainData(
        txid=txid,
        block_height=tx.get("status", {}).get("block_height") or _get_height_from_rpc(tx),
        block_timestamp=datetime.fromtimestamp(block_time, tz=timezone.utc),
        fee_sats=fee_sats,
        fee_btc=Decimal(fee_sats) / SATS_PER_BTC,
        total_input_sats=sum(v.get("prevout", {}).get("value", 0) for v in tx["vin"]),
        total_output_sats=sum(v["value"] for v in tx["vout"]),
        input_addresses=[
            v.get("prevout", {}).get("scriptpubkey_address", "unknown")
            for v in tx["vin"]
        ],
        output_addresses=[
            v.get("scriptpubkey_address", "unknown")
            for v in tx["vout"]
        ],
        confirmations=tx.get("confirmations", 0),
        is_confirmed=tx.get("status", {}).get("confirmed", False),
    )
```

### Step 2: Cross-validate against exchange/wallet data

```python
from datetime import timedelta

@dataclass
class VerificationResult:
    txid: str
    timestamp_match: bool
    timestamp_diff_seconds: int        # exchange time vs block time
    amount_match: bool
    amount_diff_btc: Decimal           # should be zero or equal to fee
    fee_verified: bool
    fee_btc: Decimal                   # exact on-chain fee
    exchange_reported_fee: Optional[Decimal]
    discrepancies: list[str]           # human-readable list of issues

def verify_transaction(imported_tx, on_chain: OnChainData) -> VerificationResult:
    """Compare imported exchange/wallet data against on-chain truth."""
    discrepancies = []

    # Timestamp check: exchange timestamp vs block confirmation time
    time_diff = abs(
        (imported_tx.timestamp - on_chain.block_timestamp).total_seconds()
    )
    timestamp_ok = time_diff < 7200  # 2 hours tolerance for normal BTC confirmation
    if not timestamp_ok:
        discrepancies.append(
            f"Timestamp mismatch: exchange={imported_tx.timestamp.isoformat()}, "
            f"on-chain={on_chain.block_timestamp.isoformat()}, "
            f"diff={time_diff:.0f}s"
        )

    # Amount check: for withdrawals, the on-chain output to the destination
    # should match the imported amount (exchange amount minus fee)
    amount_diff = abs(imported_tx.amount_btc - _find_relevant_output(on_chain, imported_tx))
    amount_ok = amount_diff < Decimal("0.00000001")  # 1 sat tolerance
    if not amount_ok:
        discrepancies.append(
            f"Amount mismatch: imported={imported_tx.amount_btc}, "
            f"on-chain output={_find_relevant_output(on_chain, imported_tx)}, "
            f"diff={amount_diff}"
        )

    # Fee verification
    fee_ok = True
    if imported_tx.fee_btc is not None:
        fee_diff = abs(imported_tx.fee_btc - on_chain.fee_btc)
        fee_ok = fee_diff < Decimal("0.00000001")
        if not fee_ok:
            discrepancies.append(
                f"Fee mismatch: imported={imported_tx.fee_btc}, "
                f"on-chain={on_chain.fee_btc}"
            )

    return VerificationResult(
        txid=on_chain.txid,
        timestamp_match=timestamp_ok,
        timestamp_diff_seconds=int(time_diff),
        amount_match=amount_ok,
        amount_diff_btc=amount_diff,
        fee_verified=fee_ok,
        fee_btc=on_chain.fee_btc,
        exchange_reported_fee=imported_tx.fee_btc,
        discrepancies=discrepancies,
    )
```

### Step 3: Definitive transfer matching

Instead of heuristic matching, use the txid to prove two records are the same on-chain event:

```python
def match_transfers(withdrawals: list, deposits: list, api) -> list[dict]:
    """
    Match exchange withdrawals to wallet deposits using on-chain txid.

    A withdrawal from Shakepay and a deposit into Sparrow with the same txid
    are definitively the same transaction — no ambiguity.
    """
    matches = []
    deposit_by_txid = {d.txid: d for d in deposits if d.txid}

    for withdrawal in withdrawals:
        if not withdrawal.txid:
            continue

        # Direct txid match — irrefutable proof of same transaction
        if withdrawal.txid in deposit_by_txid:
            deposit = deposit_by_txid[withdrawal.txid]
            on_chain = enrich_transaction(withdrawal.txid, api)

            matches.append({
                "type": "verified_transfer",
                "txid": withdrawal.txid,
                "withdrawal": withdrawal,
                "deposit": deposit,
                "on_chain": on_chain,
                "fee_btc": on_chain.fee_btc,
                "block_timestamp": on_chain.block_timestamp,
                "is_disposition": False,  # transfer between own wallets = NOT taxable
            })

    return matches
```

### Step 4: Override imported data with on-chain values

When discrepancies exist, on-chain data takes precedence:

```python
def apply_on_chain_overrides(imported_tx, on_chain: OnChainData, verification: VerificationResult):
    """
    Override imported values with authoritative on-chain data.
    Log all overrides for audit trail.
    """
    overrides = []

    # Always use block timestamp for tax date (most defensible)
    if imported_tx.timestamp != on_chain.block_timestamp:
        overrides.append({
            "field": "timestamp",
            "old": imported_tx.timestamp.isoformat(),
            "new": on_chain.block_timestamp.isoformat(),
            "reason": "Block confirmation timestamp is authoritative for CRA"
        })
        imported_tx.timestamp = on_chain.block_timestamp

    # Use on-chain fee when exchange didn't report one (e.g., Sparrow, Shakepay)
    if imported_tx.fee_btc is None or not verification.fee_verified:
        overrides.append({
            "field": "fee_btc",
            "old": str(imported_tx.fee_btc),
            "new": str(on_chain.fee_btc),
            "reason": "On-chain fee is exact; exchange CSV was missing or inaccurate"
        })
        imported_tx.fee_btc = on_chain.fee_btc

    return overrides
```

---

## Tax-Relevant Use Cases

### 1. Accurate fee capture for Sparrow transactions

Sparrow's CSV does not include fees on outgoing transactions. The node provides the exact fee, which is needed for either:
- **Micro-disposition approach**: The fee portion of BTC is a taxable disposition. Capital gain/loss = FMV of fee at block time minus ACB of fee portion.
- **ACB addition approach**: Fee is added to the ACB of the receiving wallet's holdings.

```python
def calculate_transfer_fee_disposition(on_chain: OnChainData, acb_per_unit: Decimal, cad_rate: Decimal):
    """Calculate gain/loss on the network fee treated as a micro-disposition."""
    fee_btc = on_chain.fee_btc
    fee_fmv_cad = fee_btc * cad_rate  # FMV at block confirmation time
    fee_acb_cad = fee_btc * acb_per_unit
    gain_loss = fee_fmv_cad - fee_acb_cad

    return {
        "type": "fee_disposition",
        "txid": on_chain.txid,
        "fee_btc": fee_btc,
        "fmv_cad": fee_fmv_cad,
        "acb_cad": fee_acb_cad,
        "gain_loss_cad": gain_loss,
        "block_timestamp": on_chain.block_timestamp,
    }
```

### 2. Timestamp correction for volatile days

If BTC price moved 5%+ between the exchange-reported timestamp and the block confirmation time, using the wrong timestamp could meaningfully change the FMV and thus the reported gain/loss. The node provides the correct timestamp.

### 3. Tax year boundary transactions

For transactions near December 31 / January 1, the block confirmation time determines which tax year the disposition falls into. An exchange might show a withdrawal initiated on Dec 31 at 11:55 PM, but the block confirms on Jan 1 at 12:10 AM — that's a different tax year entirely.

```python
def check_year_boundary(imported_tx, on_chain: OnChainData) -> Optional[str]:
    """Flag transactions where the tax year might differ based on data source."""
    imported_year = imported_tx.timestamp.year
    onchain_year = on_chain.block_timestamp.year
    if imported_year != onchain_year:
        return (
            f"TAX YEAR CONFLICT: Exchange reports {imported_year}, "
            f"block confirms in {onchain_year}. "
            f"On-chain timestamp ({on_chain.block_timestamp.isoformat()}) "
            f"should be used for tax purposes."
        )
    return None
```

### 4. Audit-ready provenance trail

```python
@dataclass
class AuditRecord:
    """One record per transaction, stored for CRA's 6-year retention requirement."""
    txid: str
    block_height: int
    block_timestamp: str               # ISO 8601 UTC
    exchange_source: str               # "shakepay", "ndax", "sparrow", "koinly"
    exchange_timestamp: str            # ISO 8601 as reported by exchange
    amount_btc: str                    # Decimal as string for precision
    fee_btc: str
    fee_source: str                    # "on-chain" or "exchange-reported"
    cad_rate_source: str               # "bank-of-canada" with date
    cad_value: str
    verification_status: str           # "verified", "discrepancy-overridden", "unverified"
    discrepancies: list[str]
    override_log: list[dict]
```

---

## Configuration

```python
# Node verification config — add to main tool config
NODE_CONFIG = {
    # Choose one backend
    "backend": "mempool",              # "rpc" or "mempool"

    # Bitcoin Core RPC settings
    "rpc_url": "http://127.0.0.1:8332",
    "rpc_user": "",                    # from bitcoin.conf
    "rpc_password": "",                # from bitcoin.conf

    # Mempool API settings
    "mempool_url": "http://localhost:3006/api",  # self-hosted
    # "mempool_url": "https://mempool.space/api",  # public fallback

    # Verification behavior
    "verify_all_txids": True,          # verify every tx with a txid
    "override_timestamps": True,       # use block time instead of exchange time
    "override_fees": True,             # use on-chain fee when exchange is missing/wrong
    "flag_year_boundary": True,        # alert on Dec/Jan tax year conflicts
    "timestamp_tolerance_seconds": 7200,  # 2h before flagging as discrepancy
    "cache_results": True,             # cache node responses to avoid repeated queries
    "cache_dir": ".node_cache/",       # local cache directory
}
```

---

## Dependencies and Prerequisites

| Requirement | Purpose | Notes |
|-------------|---------|-------|
| Bitcoin Core with `txindex=1` | Full transaction lookup via RPC | If not already indexed, requires `bitcoind -reindex` (several hours) |
| **OR** Mempool instance | REST API backed by your node | Easier API, includes fee calculation, address decoding |
| `requests` Python package | HTTP calls to RPC/API | Already a dependency of the main tool |
| Network access to node | Localhost only (no external calls) | Fully private — no transaction data leaves your machine |

---

## Integration with Main Pipeline

This module slots into the existing processing pipeline from the main spec:

```
1. Import        → Parse CSVs from Shakepay, NDAX, Sparrow, Koinly
2. Normalize     → Convert to CAD, standardize timestamps
3. Classify      → Tag as buy/sell/trade/transfer/income/fee
   ┌─────────────────────────────────────────────────┐
   │ 3.5 NODE VERIFICATION (this module)             │
   │   a. Enrich each tx with on-chain data          │
   │   b. Cross-validate amounts, timestamps, fees   │
   │   c. Definitive transfer matching via txid      │
   │   d. Override imported data with on-chain truth  │
   │   e. Flag year-boundary conflicts               │
   │   f. Write audit trail records                  │
   └─────────────────────────────────────────────────┘
4. Match transfers  → Now uses node-verified matches (much higher confidence)
5. Calculate ACB    → Uses corrected timestamps and exact fees
6. Detect superficial losses → Uses corrected dates for 61-day window
7. Generate reports → Includes verification status per transaction
```

The module is **optional but strongly recommended**. If the node is unavailable, the pipeline falls back to exchange/wallet data only and logs a warning. All verification results are stored alongside the transaction data for the 6-year CRA record retention period.