"""Message codes for all user-facing output in sirop.

Each value is the dotted key used to look up the message entry in
``config/messages.yaml``. Use these codes with ``sirop.utils.messages.emit()``.

Naming convention:
  <command>.<status>.<topic>   e.g. tap.error.file_not_found
  <shared>.<status>.<topic>    e.g. batch.error.no_active
"""

from enum import StrEnum


class MessageCode(StrEnum):
    # ── Batch / shared ────────────────────────────────────────────────────────
    BATCH_ERROR_NO_ACTIVE = "batch.error.no_active"

    # ── create ────────────────────────────────────────────────────────────────
    CREATE_ERROR_NO_YEAR = "create.error.no_year"
    CREATE_ERROR_BATCH_EXISTS = "create.error.batch_exists"
    CREATE_ERROR_DB_INIT = "create.error.db_init"
    CREATE_BATCH_CREATED = "create.batch_created"

    # ── tap ───────────────────────────────────────────────────────────────────
    TAP_ERROR_FILE_NOT_FOUND = "tap.error.file_not_found"
    TAP_ERROR_NO_HEADER = "tap.error.no_header"
    TAP_ERROR_NO_IMPORTER = "tap.error.no_importer"
    TAP_ERROR_PARSE_FAILED = "tap.error.parse_failed"
    TAP_ERROR_NO_FORMAT_MATCH = "tap.error.no_format_match"
    TAP_ERROR_MULTIPLE_FORMATS = "tap.error.multiple_formats"
    TAP_ERROR_MISSING_COLUMNS = "tap.error.missing_columns"
    TAP_ERROR_STAGE_RUNNING = "tap.error.stage_running"
    TAP_ERROR_WRITE_FAILED = "tap.error.write_failed"
    TAP_FORMAT_DETECTED = "tap.format_detected"
    TAP_HEADERS_FOUND = "tap.headers_found"
    TAP_HINT_CLOSEST_MATCH = "tap.hint.closest_match"
    TAP_KNOWN_FORMATS = "tap.known_formats"
    TAP_MISSING_COLUMN = "tap.missing_column"
    TAP_HINT_SUGGESTED_SOURCE = "tap.hint.suggested_source"
    TAP_EXPECTED_COLUMNS = "tap.expected_columns"
    TAP_NOTHING_NEW = "tap.nothing_new"
    TAP_SUCCESS = "tap.success"
    TAP_FOLDER_NO_FILES = "tap.folder.no_files"
    TAP_FOLDER_HEADER = "tap.folder.header"
    TAP_FOLDER_FILE_DETECTED = "tap.folder.file_detected"
    TAP_FOLDER_FILE_UNKNOWN = "tap.folder.file_unknown"
    TAP_FOLDER_ALL_UNKNOWN = "tap.folder.all_unknown"
    TAP_FOLDER_ABORTED = "tap.folder.aborted"
    TAP_WALLET_CONFLICT_ABORTED = "tap.wallet.conflict_aborted"

    # ── boil ──────────────────────────────────────────────────────────────────
    BOIL_ERROR_NOT_TAPPED = "boil.error.not_tapped"
    BOIL_ERROR_NO_RAW_TRANSACTIONS = "boil.error.no_raw_transactions"
    BOIL_ERROR_NO_TAX_RULES = "boil.error.no_tax_rules"
    BOIL_ERROR_STAGE_RUNNING = "boil.error.stage_running"
    BOIL_ERROR_UNKNOWN_STAGE = "boil.error.unknown_stage"
    BOIL_NORMALIZE_PREFETCH_BOC = "boil.normalize.prefetch_boc"
    BOIL_NORMALIZE_PREFETCH_CRYPTO = "boil.normalize.prefetch_crypto"
    BOIL_NORMALIZE_COINGECKO_ATTRIBUTION = "boil.normalize.coingecko_attribution"
    BOIL_NORMALIZE_BOC_ATTRIBUTION = "boil.normalize.boc_attribution"
    BOIL_NORMALIZE_MEMPOOL_ATTRIBUTION = "boil.normalize.mempool_attribution"
    BOIL_NORMALIZE_ZERO_CAD_WARNING = "boil.normalize.zero_cad_warning"
    BOIL_TRANSFER_MATCH_UNMATCHED_WITHDRAWAL = "boil.transfer_match.unmatched_withdrawal"
    BOIL_TRANSFER_MATCH_UNMATCHED_DEPOSIT = "boil.transfer_match.unmatched_deposit"
    BOIL_WARNING_FUTURE_YEAR_DISPOSITIONS = "boil.warning.future_year_dispositions"
    BOIL_SUMMARY_COMPLETE = "boil.summary.complete"
    BOIL_SUMMARY_HOLDINGS_HEADER = "boil.summary.holdings_header"
    BOIL_SUMMARY_WALLET_HEADER = "boil.summary.wallet_header"
    BOIL_SUMMARY_STIR_HINT = "boil.summary.stir_hint"
    BOIL_AUDIT_WRITTEN = "boil.audit.written"
    BOIL_AUDIT_ERROR_NOT_READY = "boil.audit.error.not_ready"

    # ── stir ──────────────────────────────────────────────────────────────────
    STIR_ERROR_NOT_NORMALIZED = "stir.error.not_normalized"
    STIR_ERROR_TX_NOT_FOUND = "stir.error.tx_not_found"
    STIR_LINK_APPLIED = "stir.link_applied"
    STIR_UNLINK_APPLIED = "stir.unlink_applied"
    STIR_CLEAR_APPLIED = "stir.clear_applied"
    STIR_EXIT = "stir.exit"

    # ── list ──────────────────────────────────────────────────────────────────
    LIST_NO_BATCHES = "list.no_batches"
    LIST_BATCH_ITEM = "list.batch_item"

    # ── switch ────────────────────────────────────────────────────────────────
    SWITCH_ERROR_BATCH_NOT_FOUND = "switch.error.batch_not_found"
    SWITCH_ACTIVATED = "switch.activated"
