"""Tests for manual reconciliation adjustments.

Covers:
- v12 schema migration creates the manual_adjustments table.
- Repository CRUD for manual adjustments and audit_log.
- Boil pipeline injects manual adjustments into the ACB engine.
- Stir CLI adjust handler validates inputs (reason required, positive amounts).
- Pour reports flag manual entries.

All test data uses fake assets, amounts, and reasons — nothing real.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from sirop.cli.stir import (
    _apply_adjust,
    _apply_clear_adjustment,
    _parse_iso_date,
    _parse_positive_decimal,
    _StirError,
)
from sirop.db import repositories as repo
from sirop.db.schema import (
    SCHEMA_VERSION,
    create_tables,
    migrate_to_v11,
    migrate_to_v12,
)
from sirop.models.adjustment import ManualAdjustment
from sirop.models.messages import MessageCode


def _make_conn() -> sqlite3.Connection:
    """In-memory SQLite connection with the full sirop v12 schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchemaV12:
    def test_schema_version_is_12(self) -> None:
        assert SCHEMA_VERSION == 12  # noqa: PLR2004

    def test_manual_adjustments_table_exists(self) -> None:
        conn = _make_conn()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='manual_adjustments'"
        ).fetchall()
        assert len(rows) == 1

    def test_migrate_v12_is_idempotent(self) -> None:
        conn = _make_conn()
        migrate_to_v12(conn)
        migrate_to_v12(conn)  # second call must not error

    def test_v12_migration_on_pre_v11_db(self) -> None:
        """Opening a database that only has v11 schema and migrating to v12 must succeed."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        create_tables(conn)
        migrate_to_v11(conn)
        migrate_to_v12(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='manual_adjustments'"
        ).fetchall()
        assert len(rows) == 1

    def test_kind_check_constraint(self) -> None:
        """The CHECK constraint must reject kinds other than 'acquire' or 'dispose'."""
        conn = _make_conn()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO manual_adjustments"
                " (kind, asset, units, cad_value, timestamp, reason, created_at)"
                " VALUES ('rogue', 'BTC', '1', '100', '2024-01-01T00:00:00+00:00',"
                " 'r', '2024-01-01T00:00:00+00:00')"
            )


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class TestManualAdjustmentRepo:
    def test_write_and_read_round_trip(self) -> None:
        conn = _make_conn()
        ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        adj = repo.write_manual_adjustment(
            conn,
            kind="acquire",
            asset="BTC",
            units=Decimal("0.5"),
            cad_value=Decimal("12345.67"),
            timestamp=ts,
            reason="Lost defunct-exchange CSV; reconstructed from bank statement.",
        )
        assert adj.id > 0

        rows = repo.read_manual_adjustments(conn)
        assert len(rows) == 1
        loaded = rows[0]
        assert loaded.kind == "acquire"
        assert loaded.asset == "BTC"
        assert loaded.units == Decimal("0.5")
        assert loaded.cad_value == Decimal("12345.67")
        assert loaded.timestamp == ts
        assert "defunct-exchange" in loaded.reason

    def test_delete_returns_removed_row(self) -> None:
        conn = _make_conn()
        ts = datetime(2024, 6, 15, tzinfo=UTC)
        adj = repo.write_manual_adjustment(
            conn,
            kind="dispose",
            asset="BTC",
            units=Decimal("0.1"),
            cad_value=Decimal("500"),
            timestamp=ts,
            reason="Lost private key; treating as deemed disposition at FMV.",
        )
        removed = repo.delete_manual_adjustment(conn, adj.id)
        assert removed is not None
        assert removed.id == adj.id
        assert removed.kind == "dispose"
        assert repo.read_manual_adjustments(conn) == []

    def test_delete_missing_returns_none(self) -> None:
        conn = _make_conn()
        assert repo.delete_manual_adjustment(conn, 999) is None

    def test_decimal_storage_uses_format_f(self) -> None:
        """Storing a tiny amount must not produce scientific notation."""
        conn = _make_conn()
        ts = datetime(2024, 6, 15, tzinfo=UTC)
        repo.write_manual_adjustment(
            conn,
            kind="acquire",
            asset="BTC",
            units=Decimal("0.00000001"),
            cad_value=Decimal("0.0001"),
            timestamp=ts,
            reason="dust adjustment",
        )
        row = conn.execute("SELECT units, cad_value FROM manual_adjustments").fetchone()
        assert "E" not in row["units"].upper()
        assert "E" not in row["cad_value"].upper()


class TestAuditLog:
    def test_append_and_read(self) -> None:
        conn = _make_conn()
        log_id = repo.write_audit_log(
            conn,
            stage="manual_adjust",
            field="acb_pool:BTC",
            old_value=None,
            new_value="acquire:0.5 BTC @ 12000 CAD on 2024-06-15",
            reason="Lost defunct-exchange CSV.",
        )
        assert log_id > 0
        rows = repo.read_audit_log(conn)
        assert len(rows) == 1
        assert rows[0]["stage"] == "manual_adjust"
        assert rows[0]["field"] == "acb_pool:BTC"
        assert "Lost" in (rows[0]["reason"] or "")

    def test_clear_writes_second_audit_row(self) -> None:
        """Removing an adjustment must produce an additional audit_log entry,
        not delete the original entry."""
        conn = _make_conn()
        ts = datetime(2024, 6, 15, tzinfo=UTC)
        adj = repo.write_manual_adjustment(
            conn,
            kind="acquire",
            asset="BTC",
            units=Decimal("0.5"),
            cad_value=Decimal("12345.67"),
            timestamp=ts,
            reason="initial entry",
        )
        # Simulate the CLI flow: write audit on add, then write audit on remove.
        repo.write_audit_log(
            conn,
            stage="manual_adjust",
            field="acb_pool:BTC",
            new_value="entry created",
            reason="initial entry",
        )
        _apply_clear_adjustment(conn, adj.id)
        rows = repo.read_audit_log(conn)
        assert len(rows) == 2  # noqa: PLR2004 — two events recorded
        stages = {r["stage"] for r in rows}
        assert stages == {"manual_adjust", "manual_adjust_clear"}


# ---------------------------------------------------------------------------
# Stir handler input validation
# ---------------------------------------------------------------------------


class TestStirAdjustValidation:
    def test_reason_required(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_adjust(conn, "acquire", ("BTC", "0.5", "100", "2024-06-15"), reason="   ")
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_REASON_REQUIRED

    def test_invalid_asset_rejected(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_adjust(conn, "acquire", ("not-an-asset", "0.5", "100", "2024-06-15"), reason="r")
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_INVALID_ASSET

    def test_negative_units_rejected(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_adjust(conn, "acquire", ("BTC", "-1.0", "100", "2024-06-15"), reason="r")
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_INVALID_AMOUNT

    def test_zero_cad_rejected(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_adjust(conn, "acquire", ("BTC", "0.5", "0", "2024-06-15"), reason="r")
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_INVALID_AMOUNT

    def test_invalid_date_rejected(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_adjust(conn, "acquire", ("BTC", "0.5", "100", "yesterday"), reason="r")
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_INVALID_DATE

    def test_clear_unknown_id_raises(self) -> None:
        conn = _make_conn()
        with pytest.raises(_StirError) as exc:
            _apply_clear_adjustment(conn, 4242)
        assert exc.value.msg_code == MessageCode.STIR_ERROR_ADJUST_NOT_FOUND

    def test_happy_path_writes_adjustment_and_audit(self) -> None:
        conn = _make_conn()
        rc = _apply_adjust(
            conn,
            "acquire",
            ("btc", "0.50000000", "12345.67", "2024-06-15"),
            reason="Lost private key on old laptop; reconstructed from bank statement.",
        )
        assert rc == 0
        adjustments = repo.read_manual_adjustments(conn)
        assert len(adjustments) == 1
        assert adjustments[0].asset == "BTC"  # uppercased
        audit = repo.read_audit_log(conn)
        assert len(audit) == 1
        assert audit[0]["stage"] == "manual_adjust"


class TestParsers:
    @pytest.mark.parametrize(
        ("raw", "expected_year"),
        [
            ("2024-06-15", 2024),
            ("2024-06-15T12:00:00Z", 2024),
            ("2024-06-15T12:00:00+00:00", 2024),
        ],
    )
    def test_parse_iso_date_accepts_valid(self, raw: str, expected_year: int) -> None:
        dt = _parse_iso_date(raw)
        assert dt is not None
        assert dt.year == expected_year
        assert dt.tzinfo is not None

    @pytest.mark.parametrize("raw", ["", "  ", "not-a-date", "2024/06/15", "06-15-2024"])
    def test_parse_iso_date_rejects_invalid(self, raw: str) -> None:
        assert _parse_iso_date(raw) is None

    @pytest.mark.parametrize(
        ("raw", "ok"),
        [("0.5", True), ("100", True), ("0", False), ("-1", False), ("abc", False), ("", False)],
    )
    def test_parse_positive_decimal(self, raw: str, ok: bool) -> None:
        result = _parse_positive_decimal(raw)
        if ok:
            assert result is not None
            assert result > Decimal("0")
        else:
            assert result is None


# ---------------------------------------------------------------------------
# Pipeline integration — synthetic events flow through ACB engine
# ---------------------------------------------------------------------------


class TestBoilInjection:
    def test_inject_manual_adjustments_writes_classified_events(self) -> None:
        from sirop.cli.boil import _inject_manual_adjustments

        conn = _make_conn()
        ts = datetime(2024, 6, 15, tzinfo=UTC)
        repo.write_manual_adjustment(
            conn,
            kind="acquire",
            asset="BTC",
            units=Decimal("0.5"),
            cad_value=Decimal("12345.67"),
            timestamp=ts,
            reason="reconstructed from bank statement",
        )
        repo.write_manual_adjustment(
            conn,
            kind="dispose",
            asset="BTC",
            units=Decimal("0.1"),
            cad_value=Decimal("500"),
            timestamp=ts,
            reason="lost private key — deemed disposition at FMV",
        )
        count = _inject_manual_adjustments(conn)
        assert count == 2  # noqa: PLR2004

        events = repo.read_classified_events(conn)
        assert len(events) == 2  # noqa: PLR2004
        kinds = {e.event_type for e in events}
        assert kinds == {"buy", "sell"}
        assert all(e.source == "manual" for e in events)
        assert all(e.is_provisional for e in events)
        assert all(e.is_taxable for e in events)
        assert all(e.vtx_id is None for e in events)

    def test_acb_engine_processes_manual_events(self) -> None:
        """Manual buy then sell should produce a normal disposition with correct gain."""
        from sirop.engine.acb import TaxRules, run

        ts1 = datetime(2024, 1, 15, tzinfo=UTC)
        ts2 = datetime(2024, 2, 15, tzinfo=UTC)

        manual_buy = ManualAdjustment(
            id=1,
            kind="acquire",
            asset="BTC",
            units=Decimal("1.0"),
            cad_value=Decimal("10000"),
            timestamp=ts1,
            reason="initial recovery",
            created_at=ts1,
        )
        manual_sell = ManualAdjustment(
            id=2,
            kind="dispose",
            asset="BTC",
            units=Decimal("0.5"),
            cad_value=Decimal("8000"),
            timestamp=ts2,
            reason="documented loss",
            created_at=ts2,
        )

        # Convert to ClassifiedEvent the same way _inject_manual_adjustments does.
        from sirop.models.event import ClassifiedEvent

        events = [
            ClassifiedEvent(
                id=1,
                vtx_id=None,
                timestamp=manual_buy.timestamp,
                event_type="buy",
                asset=manual_buy.asset,
                amount=manual_buy.units,
                cad_proceeds=None,
                cad_cost=manual_buy.cad_value,
                cad_fee=None,
                txid=None,
                source="manual",
                is_taxable=True,
                wallet_id=None,
                is_provisional=True,
            ),
            ClassifiedEvent(
                id=2,
                vtx_id=None,
                timestamp=manual_sell.timestamp,
                event_type="sell",
                asset=manual_sell.asset,
                amount=manual_sell.units,
                cad_proceeds=manual_sell.cad_value,
                cad_cost=None,
                cad_fee=None,
                txid=None,
                source="manual",
                is_taxable=True,
                wallet_id=None,
                is_provisional=True,
            ),
        ]
        rules = TaxRules(
            capital_gains_inclusion_rate=Decimal("0.50"),
            superficial_loss_window_days=30,
            reward_treatment={},
        )
        disps, _states, pools, _last, underruns = run(events, rules)

        assert underruns == []
        assert len(disps) == 1
        d = disps[0]
        # ACB of disposed = 10000 / 1.0 * 0.5 = 5000; gain = 8000 - 5000 = 3000
        assert d.acb_of_disposed_cad == Decimal("5000.00000000")
        assert d.gain_loss_cad == Decimal("3000.00000000")
        # Pool after has 0.5 BTC at 5000 CAD basis.
        assert pools["BTC"].total_units == Decimal("0.5")
        assert pools["BTC"].total_acb_cad == Decimal("5000")


# ---------------------------------------------------------------------------
# Report flagging
# ---------------------------------------------------------------------------


class TestReportFlagging:
    def test_dispositions_table_marks_manual_rows(self) -> None:
        from sirop.models.disposition import AdjustedDisposition
        from sirop.reports.formatter import _build_dispositions_table

        ts = datetime(2024, 6, 15, tzinfo=UTC)
        d_normal = AdjustedDisposition(
            id=1,
            disposition_id=1,
            timestamp=ts,
            asset="BTC",
            units=Decimal("0.1"),
            proceeds_cad=Decimal("500"),
            acb_of_disposed_cad=Decimal("400"),
            selling_fees_cad=Decimal("0"),
            gain_loss_cad=Decimal("100"),
            is_superficial_loss=False,
            superficial_loss_denied_cad=Decimal("0"),
            allowable_loss_cad=Decimal("0"),
            adjusted_gain_loss_cad=Decimal("100"),
            adjusted_acb_of_repurchase_cad=None,
            disposition_type="sell",
            year_acquired="2024",
        )
        d_manual = AdjustedDisposition(
            id=2,
            disposition_id=2,
            timestamp=ts,
            asset="BTC",
            units=Decimal("0.05"),
            proceeds_cad=Decimal("250"),
            acb_of_disposed_cad=Decimal("200"),
            selling_fees_cad=Decimal("0"),
            gain_loss_cad=Decimal("50"),
            is_superficial_loss=False,
            superficial_loss_denied_cad=Decimal("0"),
            allowable_loss_cad=Decimal("0"),
            adjusted_gain_loss_cad=Decimal("50"),
            adjusted_acb_of_repurchase_cad=None,
            disposition_type="sell",
            year_acquired="2024",
        )
        out = _build_dispositions_table([d_normal, d_manual], 2024, manual_disposition_ids={2})
        assert "Manual reconciliation entry" in out
        # The manual row should carry the flag, the normal row should not.
        manual_line = next(
            line for line in out.splitlines() if "Manual reconciliation entry" in line
        )
        assert "0.05" in manual_line or "50" in manual_line

    def test_acquisitions_table_flags_manual_assets(self) -> None:
        from sirop.models.event import ClassifiedEvent
        from sirop.reports.formatter import _build_tp_part3_acquisitions

        ts = datetime(2024, 6, 15, tzinfo=UTC)
        normal = ClassifiedEvent(
            id=10,
            vtx_id=10,
            timestamp=ts,
            event_type="buy",
            asset="BTC",
            amount=Decimal("0.5"),
            cad_proceeds=None,
            cad_cost=Decimal("10000"),
            cad_fee=None,
            txid=None,
            source="shakepay",
            is_taxable=True,
        )
        manual = ClassifiedEvent(
            id=99,
            vtx_id=None,
            timestamp=ts,
            event_type="buy",
            asset="BTC",
            amount=Decimal("0.1"),
            cad_proceeds=None,
            cad_cost=Decimal("1000"),
            cad_fee=None,
            txid=None,
            source="manual",
            is_taxable=True,
            is_provisional=True,
        )
        out = _build_tp_part3_acquisitions([normal, manual], 2024, manual_event_ids={99})
        assert "manual reconciliation" in out.lower()
