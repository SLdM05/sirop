"""Unit tests for the shared cross-batch asset price cache.

All tests use in-memory SQLite connections -- no filesystem I/O required
(except for open_price_cache tests which use tmp_path).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import TYPE_CHECKING

from sirop.utils.price_cache import (
    _BOC_RATES_DDL,
    _CRYPTO_PRICES_DDL,
    PRICE_CACHE_FILENAME,
    copy_prices_into_batch,
    open_price_cache,
    sync_prices_to_cache,
)

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn_with_tables() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the two price tables."""
    conn = sqlite3.connect(":memory:")
    conn.execute(_BOC_RATES_DDL)
    conn.execute(_CRYPTO_PRICES_DDL)
    conn.commit()
    return conn


def _insert_boc(
    conn: sqlite3.Connection,
    date_str: str,
    pair: str = "USDCAD",
    rate: str = "1.3500",
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO boc_rates"
        " (date, currency_pair, rate, fetched_at) VALUES (?, ?, ?, ?)",
        (date_str, pair, rate, "2025-01-01T00:00:00Z"),
    )
    conn.commit()


def _insert_crypto(
    conn: sqlite3.Connection,
    date_str: str,
    asset: str = "BTC",
    price_usd: str = "50000.00",
    price_cad: str = "67500.00",
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO crypto_prices"
        " (date, asset, price_usd, price_cad, source, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (date_str, asset, price_usd, price_cad, "mempool", "2025-01-01T00:00:00Z"),
    )
    conn.commit()


def _boc_dates(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT date FROM boc_rates")}


def _crypto_dates(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT date FROM crypto_prices")}


# ---------------------------------------------------------------------------
# open_price_cache
# ---------------------------------------------------------------------------


def test_open_price_cache_creates_file_and_tables(tmp_path: Path) -> None:
    conn = open_price_cache(tmp_path)
    try:
        assert (tmp_path / PRICE_CACHE_FILENAME).exists()
        conn.execute("SELECT 1 FROM boc_rates LIMIT 1")
        conn.execute("SELECT 1 FROM crypto_prices LIMIT 1")
    finally:
        conn.close()


def test_open_price_cache_idempotent(tmp_path: Path) -> None:
    """Calling open_price_cache twice on the same directory must not raise."""
    conn1 = open_price_cache(tmp_path)
    conn1.close()
    conn2 = open_price_cache(tmp_path)
    conn2.close()


def test_open_price_cache_creates_data_dir(tmp_path: Path) -> None:
    nested = tmp_path / "deeply" / "nested"
    assert not nested.exists()
    conn = open_price_cache(nested)
    conn.close()
    assert nested.exists()


# ---------------------------------------------------------------------------
# copy_prices_into_batch
# ---------------------------------------------------------------------------


def test_copy_boc_rates_into_batch_in_range() -> None:
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    _insert_boc(cache, "2025-03-01")
    _insert_boc(cache, "2025-03-15")
    _insert_boc(cache, "2025-04-01")  # outside range

    boc_copied, crypto_copied = copy_prices_into_batch(
        cache, batch, date(2025, 3, 1), date(2025, 3, 31)
    )

    expected_boc = 2
    assert boc_copied == expected_boc
    assert crypto_copied == 0
    assert _boc_dates(batch) == {"2025-03-01", "2025-03-15"}
    assert "2025-04-01" not in _boc_dates(batch)


def test_copy_crypto_prices_into_batch_in_range() -> None:
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    _insert_crypto(cache, "2025-03-10")
    _insert_crypto(cache, "2025-04-10")  # outside range

    boc_copied, crypto_copied = copy_prices_into_batch(
        cache, batch, date(2025, 3, 1), date(2025, 3, 31)
    )

    assert boc_copied == 0
    assert crypto_copied == 1
    assert _crypto_dates(batch) == {"2025-03-10"}


def test_copy_prices_insert_or_ignore_does_not_overwrite() -> None:
    """Existing batch rows must not be overwritten by cache values."""
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    _insert_boc(cache, "2025-03-01", rate="1.9999")  # cache has different rate
    _insert_boc(batch, "2025-03-01", rate="1.3500")  # batch already has correct rate

    copy_prices_into_batch(cache, batch, date(2025, 3, 1), date(2025, 3, 31))

    row = batch.execute("SELECT rate FROM boc_rates WHERE date = '2025-03-01'").fetchone()
    assert row is not None
    assert row[0] == "1.3500"  # original batch value preserved


def test_copy_prices_empty_cache_returns_zeros() -> None:
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    boc_copied, crypto_copied = copy_prices_into_batch(
        cache, batch, date(2025, 1, 1), date(2025, 12, 31)
    )

    assert boc_copied == 0
    assert crypto_copied == 0


# ---------------------------------------------------------------------------
# sync_prices_to_cache
# ---------------------------------------------------------------------------


def test_sync_new_prices_to_cache() -> None:
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    _insert_boc(batch, "2025-06-01")
    _insert_boc(batch, "2025-06-02")
    _insert_crypto(batch, "2025-06-01")

    expected_boc = 2
    boc_synced, crypto_synced = sync_prices_to_cache(batch, cache)

    assert boc_synced == expected_boc
    assert crypto_synced == 1
    assert _boc_dates(cache) == {"2025-06-01", "2025-06-02"}
    assert _crypto_dates(cache) == {"2025-06-01"}


def test_sync_overwrites_stale_cache_entry() -> None:
    """INSERT OR REPLACE should update an existing cache row with fresher data."""
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    _insert_boc(cache, "2025-06-01", rate="1.2000")  # stale
    _insert_boc(batch, "2025-06-01", rate="1.3500")  # fresher

    sync_prices_to_cache(batch, cache)

    row = cache.execute("SELECT rate FROM boc_rates WHERE date = '2025-06-01'").fetchone()
    assert row is not None
    assert row[0] == "1.3500"


def test_sync_empty_batch_returns_zeros() -> None:
    cache = _make_conn_with_tables()
    batch = _make_conn_with_tables()

    boc_synced, crypto_synced = sync_prices_to_cache(batch, cache)

    assert boc_synced == 0
    assert crypto_synced == 0


# ---------------------------------------------------------------------------
# Round-trip: copy -> sync -> copy (simulates two sequential batches)
# ---------------------------------------------------------------------------


def test_roundtrip_second_batch_sees_first_batch_prices() -> None:
    """Prices fetched in batch A must be visible to batch B via the shared cache."""
    cache = _make_conn_with_tables()
    batch_a = _make_conn_with_tables()
    batch_b = _make_conn_with_tables()

    march_days = 5
    for day in range(1, march_days + 1):
        d = f"2025-03-{day:02d}"
        _insert_boc(batch_a, d)
        _insert_crypto(batch_a, d)

    sync_prices_to_cache(batch_a, cache)

    boc_hit, crypto_hit = copy_prices_into_batch(
        cache, batch_b, date(2025, 3, 1), date(2025, 3, 31)
    )

    assert boc_hit == march_days
    assert crypto_hit == march_days
    assert _boc_dates(batch_b) == {f"2025-03-{d:02d}" for d in range(1, march_days + 1)}
    assert _crypto_dates(batch_b) == {f"2025-03-{d:02d}" for d in range(1, march_days + 1)}


def test_roundtrip_partial_overlap() -> None:
    """Only the overlapping dates should be transferred on a partial date overlap."""
    cache = _make_conn_with_tables()
    batch_b = _make_conn_with_tables()

    # Cache covers Jan-Jun; batch B only needs Apr-Dec.
    for month in range(1, 7):
        _insert_boc(cache, f"2025-{month:02d}-15")

    expected_overlap = 3  # Apr, May, Jun
    boc_hit, _ = copy_prices_into_batch(cache, batch_b, date(2025, 4, 1), date(2025, 12, 31))

    assert boc_hit == expected_overlap
    assert _boc_dates(batch_b) == {"2025-04-15", "2025-05-15", "2025-06-15"}
