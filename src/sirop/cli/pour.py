"""Handler for ``sirop pour``.

Generates a Markdown tax report from the active batch's computed data
(dispositions_adjusted, income_events, acb_state) and writes it to
<OUTPUT_DIR>/<batch>-<year>-tax-report.md.

Usage
-----
    sirop pour                              # write report to OUTPUT_DIR
    sirop pour --output-dir /tmp/reports    # override output directory
"""

from __future__ import annotations

import importlib.metadata
from decimal import Decimal
from pathlib import Path

import yaml

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.models.messages import MessageCode
from sirop.reports.formatter import build_detail_report, build_report
from sirop.utils.logging import get_logger
from sirop.utils.messages import emit

logger = get_logger(__name__)

_TAX_RULES_PATH = Path("config/tax_rules.yaml")


class _PourError(Exception):
    def __init__(self, code: MessageCode, **kwargs: object) -> None:
        self.msg_code = code
        self.msg_kwargs = kwargs
        super().__init__(str(code))


def handle_pour(
    settings: Settings | None = None,
    output_dir_override: Path | None = None,
) -> int:
    if settings is None:
        settings = get_settings()
    try:
        return _run_pour(settings, output_dir_override)
    except _PourError as exc:
        emit(exc.msg_code, **exc.msg_kwargs)
        return 1


def _run_pour(settings: Settings, output_dir_override: Path | None) -> int:
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _PourError(MessageCode.POUR_ERROR_NO_ACTIVE)

    conn = open_batch(batch_name, settings)
    try:
        sl_status = repo.get_stage_status(conn, "superficial_loss")
        if sl_status != "done":
            raise _PourError(MessageCode.POUR_ERROR_NOT_READY, name=batch_name)

        tax_year = repo.read_tax_year(conn)
        inclusion_rate = _load_inclusion_rate()

        repo.set_stage_running(conn, "pour")

        dispositions = repo.read_adjusted_dispositions(conn)
        income_events = repo.read_income_events(conn)
        acb_final = repo.read_acb_state_final(conn)
        all_events = repo.read_classified_events(conn)
        acquisitions = [e for e in all_events if e.event_type in ("buy", "income", "other")]

        sirop_version = _get_version()

        logger.debug(
            "pour: %d dispositions, %d income events, %d year-end pools, %d acquisitions",
            len(dispositions),
            len(income_events),
            len(acb_final),
            len(acquisitions),
        )

        report = build_report(
            dispositions=dispositions,
            income_events=income_events,
            acb_final=acb_final,
            acquisitions=acquisitions,
            tax_year=tax_year,
            inclusion_rate=inclusion_rate,
            batch_name=batch_name,
            sirop_version=sirop_version,
        )

        detail = build_detail_report(
            income_events=income_events,
            acb_final=acb_final,
            dispositions=dispositions,
            tax_year=tax_year,
            batch_name=batch_name,
            sirop_version=sirop_version,
        )

        out_dir = output_dir_override or settings.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{batch_name}-{tax_year}-tax-report.md"
        detail_path = out_dir / f"{batch_name}-{tax_year}-tax-detail.md"
        out_path.write_text(report, encoding="utf-8")
        detail_path.write_text(detail, encoding="utf-8")

        repo.set_stage_done(conn, "pour")
        emit(MessageCode.POUR_REPORT_WRITTEN, path=out_path)
        emit(MessageCode.POUR_DETAIL_WRITTEN, path=detail_path)
        return 0

    finally:
        conn.close()


def _load_inclusion_rate() -> Decimal:
    with _TAX_RULES_PATH.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return Decimal(str(raw["capital_gains_inclusion_rate"]))


def _get_version() -> str:
    try:
        return importlib.metadata.version("sirop")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"
