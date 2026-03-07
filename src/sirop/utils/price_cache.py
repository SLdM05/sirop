"""Shared cross-batch asset price cache.

Stores fetched BoC FX rates and crypto prices in a single SQLite file
(DATA_DIR/.sirop_price_cache) that persists across batches.  When enabled,
the boil pipeline pre-loads matching rows into each new batch's DB before
running the API prefetch, so only missing dates hit the network.

Usage
-----
    from sirop.utils.price_cache import (
        copy_prices_into_batch,
        open_price_cache,
        sync_prices_to_cache,
    )

    cache_conn = open_price_cache(settings.data_dir)
    copy_prices_into_batch(cache_conn, batch_conn, min_date, max_date)
    # … run existing prefetch …
    sync_prices_to_cache(batch_conn, cache_conn)
    cache_conn.close()
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

PRICE_CACHE_FILENAME = ".sirop_price_cache"

_BOC_RATES_DDL = """
CREATE TABLE IF NOT EXISTS boc_rates (
    date          TEXT NOT NULL,
    currency_pair TEXT NOT NULL,
    rate          TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    PRIMARY KEY (date, currency_pair)
)"""

_CRYPTO_PRICES_DDL = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    date       TEXT NOT NULL,
    asset      TEXT NOT NULL,
    price_usd  TEXT NOT NULL,
    price_cad  TEXT NOT NULL,
    source     TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (date, asset)
)"""


def open_price_cache(data_dir: Path) -> sqlite3.Connection:
    """Open (or create) the shared price cache file and ensure tables exist.

    The file is created under *data_dir* as ``.sirop_price_cache``.  The
    leading dot keeps it out of the way of batch listing commands.

    Parameters
    ----------
    data_dir:
        Directory where batch ``.sirop`` files live (i.e. ``settings.data_dir``).

    Returns
    -------
    sqlite3.Connection
        Open connection to the shared cache.  Caller is responsible for closing it.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / PRICE_CACHE_FILENAME
    conn = sqlite3.connect(str(path))
    conn.execute(_BOC_RATES_DDL)
    conn.execute(_CRYPTO_PRICES_DDL)
    conn.commit()
    return conn


def copy_prices_into_batch(
    cache_conn: sqlite3.Connection,
    batch_conn: sqlite3.Connection,
    min_date: date,
    max_date: date,
) -> tuple[int, int]:
    """Copy cached prices for [min_date, max_date] from the shared cache into a batch DB.

    Uses ``INSERT OR IGNORE`` so existing batch rows are never overwritten.

    Parameters
    ----------
    cache_conn:
        Open connection to the shared price cache.
    batch_conn:
        Open connection to the target batch ``.sirop`` DB.
    min_date, max_date:
        Inclusive date bounds.  Rows outside this range are not copied.

    Returns
    -------
    tuple[int, int]
        ``(boc_rows_copied, crypto_rows_copied)``
    """
    min_s = min_date.isoformat()
    max_s = max_date.isoformat()

    boc_rows = cache_conn.execute(
        "SELECT date, currency_pair, rate, fetched_at FROM boc_rates WHERE date BETWEEN ? AND ?",
        (min_s, max_s),
    ).fetchall()
    batch_conn.executemany(
        "INSERT OR IGNORE INTO boc_rates (date, currency_pair, rate, fetched_at) "
        "VALUES (?, ?, ?, ?)",
        boc_rows,
    )

    crypto_rows = cache_conn.execute(
        "SELECT date, asset, price_usd, price_cad, source, fetched_at "
        "FROM crypto_prices WHERE date BETWEEN ? AND ?",
        (min_s, max_s),
    ).fetchall()
    batch_conn.executemany(
        "INSERT OR IGNORE INTO crypto_prices "
        "(date, asset, price_usd, price_cad, source, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        crypto_rows,
    )
    batch_conn.commit()
    return len(boc_rows), len(crypto_rows)


def sync_prices_to_cache(
    batch_conn: sqlite3.Connection,
    cache_conn: sqlite3.Connection,
) -> tuple[int, int]:
    """Upsert all price rows from a batch DB into the shared cache.

    Runs after the API prefetch so newly fetched prices are persisted for
    future batches.  Uses ``INSERT OR REPLACE`` so the cache always holds
    the freshest data.

    Parameters
    ----------
    batch_conn:
        Open connection to the source batch ``.sirop`` DB.
    cache_conn:
        Open connection to the shared price cache.

    Returns
    -------
    tuple[int, int]
        ``(boc_rows_synced, crypto_rows_synced)``
    """
    boc_rows = batch_conn.execute(
        "SELECT date, currency_pair, rate, fetched_at FROM boc_rates"
    ).fetchall()
    cache_conn.executemany(
        "INSERT OR REPLACE INTO boc_rates (date, currency_pair, rate, fetched_at) "
        "VALUES (?, ?, ?, ?)",
        boc_rows,
    )

    crypto_rows = batch_conn.execute(
        "SELECT date, asset, price_usd, price_cad, source, fetched_at FROM crypto_prices"
    ).fetchall()
    cache_conn.executemany(
        "INSERT OR REPLACE INTO crypto_prices "
        "(date, asset, price_usd, price_cad, source, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        crypto_rows,
    )
    cache_conn.commit()
    return len(boc_rows), len(crypto_rows)
