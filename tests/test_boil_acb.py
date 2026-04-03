"""Integration tests for the boil ACB stage and year-end holdings consistency.

All test data uses synthetic, obviously fake identifiers and amounts.
No real transaction data or .sirop files are used.

Key invariant tested:
    sum(read_per_wallet_holdings) for each asset
    == read_acb_state_final.total_units for that asset

This must hold even when the batch contains graph-traversal-matched transfer
pairs with mismatched amounts (deposit > withdrawal), which previously caused
_print_wallet_holdings to include the surplus in the per-wallet unit count
while the ACB pool correctly ignored both transfer legs.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sirop.cli.boil import _run_acb
from sirop.db import repositories as repo
from sirop.db.schema import create_tables
from sirop.engine.acb import TaxRules

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2025, 3, 1, 12, 0, 0, tzinfo=UTC)

_DEFAULT_RULES = TaxRules(
    capital_gains_inclusion_rate=Decimal("0.50"),
    superficial_loss_window_days=30,
)


def _make_conn() -> sqlite3.Connection:
    """In-memory DB with full sirop schema. FK checks off for test isolation."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    with conn:
        conn.execute(
            "INSERT INTO batch_meta (name, tax_year, created_at, sirop_version)"
            " VALUES ('test', 2025, '2025-01-01T00:00:00+00:00', '0.0.0')"
        )
        conn.execute(
            "INSERT INTO wallets (id, name, source, auto_created, created_at, note)"
            " VALUES (1, 'testwallet', 'xpub', 0, '2025-01-01T00:00:00+00:00', '')"
        )
    return conn


def _ts(offset_days: int = 0) -> str:
    return (_BASE_TS + timedelta(days=offset_days)).isoformat()


def _seed_tx(  # noqa: PLR0913
    conn: sqlite3.Connection,
    tx_id: int,
    tx_type: str,
    amount: str,
    offset_days: int = 0,
    wallet_id: int = 1,
    cad_rate: str = "100000",
) -> None:
    cad_amount = str(Decimal(amount) * Decimal(cad_rate))
    with conn:
        conn.execute(
            """
            INSERT INTO transactions
                (id, raw_id, timestamp, transaction_type, asset, amount,
                 fee_crypto, fee_currency, cad_amount, cad_fee, cad_rate,
                 txid, source, is_transfer, counterpart_id, notes, wallet_id)
            VALUES (?, NULL, ?, ?, 'BTC', ?, NULL, NULL, ?, '0', ?, ?, 'xpub', 0, NULL, '', ?)
            """,
            (
                tx_id,
                _ts(offset_days),
                tx_type,
                amount,
                cad_amount,
                cad_rate,
                f"fake-txid-{tx_id:04d}",
                wallet_id,
            ),
        )


def _seed_ce(  # noqa: PLR0913
    conn: sqlite3.Connection,
    ce_id: int,
    vtx_id: int,
    event_type: str,
    amount: str,
    offset_days: int = 0,
    is_taxable: int = 1,
    wallet_id: int = 1,
    cad_cost: str | None = None,
) -> None:
    ts = _ts(offset_days)
    with conn:
        conn.execute(
            """
            INSERT INTO classified_events
                (id, vtx_id, timestamp, event_type, asset, amount,
                 cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable, wallet_id)
            VALUES (?, ?, ?, ?, 'BTC', ?, NULL, ?, NULL, ?, 'xpub', ?, ?)
            """,
            (
                ce_id,
                vtx_id,
                ts,
                event_type,
                amount,
                cad_cost,
                f"fake-txid-{vtx_id:04d}",
                is_taxable,
                wallet_id,
            ),
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_per_wallet_matches_acb_pool_no_transfers() -> None:
    """Per-wallet units == ACB pool units when there are no transfer events."""
    conn = _make_conn()

    # One buy: 0.5 BTC at 100 000 CAD/BTC
    _seed_tx(conn, 1, "deposit", "0.5", offset_days=0)
    _seed_ce(conn, 1, 1, "buy", "0.5", offset_days=0, cad_cost="50000")

    _run_acb(conn, _DEFAULT_RULES)

    acb = repo.read_acb_state_final(conn)
    per_wallet = repo.read_per_wallet_holdings(conn, 2025)

    assert len(acb) == 1
    assert acb[0].asset == "BTC"
    assert acb[0].total_units == Decimal("0.5")

    assert len(per_wallet) == 1
    assert per_wallet[0].asset == "BTC"
    assert per_wallet[0].net_units == Decimal("0.5")


def test_per_wallet_matches_acb_pool_with_mismatched_transfer_pair() -> None:
    """Per-wallet units == ACB pool units even when a matched transfer pair has
    different amounts on each leg (deposit > withdrawal).

    Regression: graph-traversal matching can link a withdrawal to a deposit via
    an intermediate untracked wallet where additional BTC was mixed in.  The
    surplus (deposit - withdrawal) previously inflated the per-wallet unit count
    because _print_wallet_holdings summed raw transactions while the ACB pool
    correctly ignored both transfer legs.

    Scenario:
      tx1  deposit  0.5 BTC  — buy (taxable)
      tx2  withdrawal  0.001 BTC  — transfer out (matched, is_taxable=0)
      tx3  deposit     0.002 BTC  — transfer in  (matched, is_taxable=0, excess = 0.001)

    ACB pool sees only the buy → 0.5 BTC.
    Per-wallet must also show 0.5 BTC (transfer legs excluded).
    """
    conn = _make_conn()

    _seed_tx(conn, 1, "deposit", "0.5", offset_days=0)  # buy
    _seed_tx(conn, 2, "withdrawal", "0.001", offset_days=10)  # transfer out
    _seed_tx(conn, 3, "deposit", "0.002", offset_days=20)  # transfer in (excess)

    _seed_ce(conn, 1, 1, "buy", "0.5", offset_days=0, is_taxable=1, cad_cost="50000")
    _seed_ce(conn, 2, 2, "transfer", "0.001", offset_days=10, is_taxable=0)
    _seed_ce(conn, 3, 3, "transfer", "0.002", offset_days=20, is_taxable=0)

    _run_acb(conn, _DEFAULT_RULES)

    acb = repo.read_acb_state_final(conn)
    per_wallet = repo.read_per_wallet_holdings(conn, 2025)

    assert len(acb) == 1
    acb_btc = acb[0]
    assert acb_btc.asset == "BTC"
    assert acb_btc.total_units == Decimal("0.5")

    assert len(per_wallet) == 1
    pw_btc = per_wallet[0]
    assert pw_btc.asset == "BTC"
    # Must equal 0.5, not 0.5 - 0.001 + 0.002 = 0.501
    assert pw_btc.net_units == acb_btc.total_units, (
        f"Per-wallet ({pw_btc.net_units}) diverges from ACB pool ({acb_btc.total_units}). "
        "Transfer-linked transactions must be excluded from the per-wallet sum."
    )


def test_per_wallet_matches_acb_pool_buy_sell_buy() -> None:
    """Per-wallet units == ACB pool units for buy → sell → buy pattern.

    Regression: before the holdover fix, acb_state MAX(id) pointed to the
    post-sell state, causing the global block to show a stale unit count.
    """
    conn = _make_conn()

    _seed_tx(conn, 1, "deposit", "0.5", offset_days=0)
    _seed_tx(conn, 2, "withdrawal", "0.1", offset_days=10)
    _seed_tx(conn, 3, "deposit", "0.3", offset_days=20)

    _seed_ce(conn, 1, 1, "buy", "0.5", offset_days=0, is_taxable=1, cad_cost="50000")
    _seed_ce(conn, 2, 2, "sell", "0.1", offset_days=10, is_taxable=1)
    _seed_ce(conn, 3, 3, "buy", "0.3", offset_days=20, is_taxable=1, cad_cost="30000")

    _run_acb(conn, _DEFAULT_RULES)

    acb = repo.read_acb_state_final(conn)
    per_wallet = repo.read_per_wallet_holdings(conn, 2025)

    acb_btc = next(h for h in acb if h.asset == "BTC")
    pw_btc = next(r for r in per_wallet if r.asset == "BTC")

    # 0.5 - 0.1 + 0.3 = 0.7
    assert acb_btc.total_units == Decimal("0.7")
    assert pw_btc.net_units == acb_btc.total_units
