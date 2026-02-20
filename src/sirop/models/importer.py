from dataclasses import dataclass


@dataclass(frozen=True)
class ImporterConfig:
    """Shape of a loaded importer config.

    Represents a config loaded from any of the three sources:
    - Built-in: ``config/importers/<name>.yaml``
    - User registry: ``DATA_DIR/importers/<name>.yaml``
    - Embedded snapshot: ``custom_importers`` table in a ``.sirop`` file

    This is a pure data class — no loading or I/O logic lives here.
    """

    name: str
    source_name: str
    date_column: str
    date_format: str
    columns: dict[str, str]  # internal key → CSV column header
    transaction_type_map: dict[str, str]  # raw CSV value → TransactionType value
    fee_model: str  # "spread" | "explicit"
    # Ledger-style importers (e.g. NDAX AlphaPoint) group multiple rows per
    # transaction by this column.  Row-per-transaction importers leave it None.
    group_by_column: str | None = None
    # Currencies that are fiat (not crypto).  Used by ledger importers to
    # distinguish fiat deposits/withdrawals from crypto ones.
    fiat_currencies: tuple[str, ...] = ()
