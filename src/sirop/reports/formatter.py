from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from sirop.models.disposition import ACBState, AdjustedDisposition, IncomeEvent
    from sirop.models.event import ClassifiedEvent

_TORONTO = ZoneInfo("America/Toronto")


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _fmt_cad(d: Decimal) -> str:
    return f"{float(d):,.2f}"


def _fmt_units(d: Decimal) -> str:
    return f"{float(d):.8f}"


def _fmt_rate(r: Decimal) -> str:
    return f"{int(r * 100)}%"


def _to_toronto(ts: datetime) -> datetime:
    return ts.astimezone(_TORONTO)


def _date_str(ts: datetime) -> str:
    return _to_toronto(ts).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _build_dispositions_table(
    dispositions: list[AdjustedDisposition],
    tax_year: int,
) -> str:
    lines: list[str] = []

    if not dispositions:
        lines.append(f"*No dispositions recorded for {tax_year}.*")
        return "\n".join(lines)

    header = (
        "| Date | Asset | Year Acquired | Proceeds (CAD) | ACB (CAD) "
        "| Expenses (CAD) | Net Gain / Loss (CAD) | Note |"
    )
    separator = (
        "|------|-------|---------------|----------------|-----------|"
        "----------------|------------------------|------|"
    )
    lines.append(header)
    lines.append(separator)

    sorted_disp = sorted(dispositions, key=lambda d: (d.timestamp, d.id))
    for d in sorted_disp:
        note = "⚠ Superficial loss" if d.is_superficial_loss else ""
        lines.append(
            f"| {_date_str(d.timestamp)} "
            f"| {d.asset} "
            f"| {d.year_acquired} "
            f"| {_fmt_cad(d.proceeds_cad)} "
            f"| {_fmt_cad(d.acb_of_disposed_cad)} "
            f"| {_fmt_cad(d.selling_fees_cad)} "
            f"| {_fmt_cad(d.adjusted_gain_loss_cad)} "
            f"| {note} |"
        )

    return "\n".join(lines)


def _build_summary_table(
    dispositions: list[AdjustedDisposition],
    inclusion_rate: Decimal,
) -> str:
    total_proceeds = sum((d.proceeds_cad for d in dispositions), Decimal("0"))
    total_acb = sum((d.acb_of_disposed_cad for d in dispositions), Decimal("0"))
    total_fees = sum((d.selling_fees_cad for d in dispositions), Decimal("0"))
    total_gains = sum(
        (d.adjusted_gain_loss_cad for d in dispositions if d.adjusted_gain_loss_cad > Decimal("0")),
        Decimal("0"),
    )
    total_losses = sum(
        (
            -d.adjusted_gain_loss_cad
            for d in dispositions
            if d.adjusted_gain_loss_cad < Decimal("0")
        ),
        Decimal("0"),
    )
    net = sum((d.adjusted_gain_loss_cad for d in dispositions), Decimal("0"))
    taxable = max(net, Decimal("0")) * inclusion_rate
    rate_str = _fmt_rate(inclusion_rate)

    lines = [
        "| | Amount (CAD) |",
        "|--|--:|",
        f"| Total proceeds | {_fmt_cad(total_proceeds)} |",
        f"| Total ACB | {_fmt_cad(total_acb)} |",
        f"| Total selling expenses | {_fmt_cad(total_fees)} |",
        f"| Total capital gains (gains only) | {_fmt_cad(total_gains)} |",
        f"| Total capital losses (losses only) | {_fmt_cad(total_losses)} |",
        f"| **Net capital gain (loss)** | **{_fmt_cad(net)}** |",
        f"| **Taxable capital gains (\xd7{rate_str}) \u2192 T1 Line 12700**"
        f" | **{_fmt_cad(taxable)}** |",
    ]
    return "\n".join(lines)


def _build_superficial_losses_section(
    dispositions: list[AdjustedDisposition],
) -> str:
    superficial = [d for d in dispositions if d.is_superficial_loss]
    if not superficial:
        return ""

    lines = [
        "### Superficial Losses",
        "",
        "The following disposals triggered the 30-day superficial loss rule (ITA s.54). The",
        "denied loss is added to the ACB of the repurchased units and cannot be claimed for",
        "this tax year.",
        "",
        "| Date | Asset | Units | Gross Loss (CAD) | Denied (CAD) | Allowable (CAD) |",
        "|------|-------|-------|-----------------|--------------|-----------------|",
    ]

    sorted_sl = sorted(superficial, key=lambda d: (d.timestamp, d.id))
    for d in sorted_sl:
        lines.append(
            f"| {_date_str(d.timestamp)} "
            f"| {d.asset} "
            f"| {_fmt_units(d.units)} "
            f"| {_fmt_cad(d.gain_loss_cad)} "
            f"| {_fmt_cad(d.superficial_loss_denied_cad)} "
            f"| {_fmt_cad(d.allowable_loss_cad)} |"
        )

    return "\n".join(lines)


def _build_tp_part3_acquisitions(
    acquisitions: list[ClassifiedEvent],
    tax_year: int,
) -> str:
    eligible = [e for e in acquisitions if e.event_type in ("buy", "income", "other")]

    if not eligible:
        return f"*No acquisitions recorded for {tax_year}.*"

    # Group by asset
    asset_units: dict[str, Decimal] = {}
    asset_cost: dict[str, Decimal] = {}
    for e in eligible:
        asset_units[e.asset] = asset_units.get(e.asset, Decimal("0")) + e.amount
        cost = (e.cad_cost or Decimal("0")) + (e.cad_fee or Decimal("0"))
        asset_cost[e.asset] = asset_cost.get(e.asset, Decimal("0")) + cost

    lines = [
        "| Asset | Total Units Acquired | Total Cost (CAD) |",
        "|-------|---------------------|-----------------|",
    ]
    for asset in sorted(asset_units.keys()):
        lines.append(
            f"| {asset} "
            f"| {_fmt_units(asset_units[asset])} "
            f"| {_fmt_cad(asset_cost[asset])} |"
        )

    return "\n".join(lines)


def _build_tp_part4_dispositions(
    dispositions: list[AdjustedDisposition],
    tax_year: int,
) -> str:
    if not dispositions:
        return f"*No dispositions recorded for {tax_year}.*"

    # Group by asset
    asset_units: dict[str, Decimal] = {}
    asset_proceeds: dict[str, Decimal] = {}
    asset_acb: dict[str, Decimal] = {}
    asset_fees: dict[str, Decimal] = {}
    asset_gain_loss: dict[str, Decimal] = {}

    for d in dispositions:
        asset_units[d.asset] = asset_units.get(d.asset, Decimal("0")) + d.units
        asset_proceeds[d.asset] = asset_proceeds.get(d.asset, Decimal("0")) + d.proceeds_cad
        asset_acb[d.asset] = asset_acb.get(d.asset, Decimal("0")) + d.acb_of_disposed_cad
        asset_fees[d.asset] = asset_fees.get(d.asset, Decimal("0")) + d.selling_fees_cad
        asset_gain_loss[d.asset] = (
            asset_gain_loss.get(d.asset, Decimal("0")) + d.adjusted_gain_loss_cad
        )

    lines = [
        "| Asset | Units Disposed | Proceeds (CAD) | ACB (CAD)"
        " | Expenses (CAD) | Net Gain / Loss (CAD) |",
        "|-------|---------------|----------------|-----------|"
        "----------------|----------------------|",
    ]
    for asset in sorted(asset_units.keys()):
        lines.append(
            f"| {asset} "
            f"| {_fmt_units(asset_units[asset])} "
            f"| {_fmt_cad(asset_proceeds[asset])} "
            f"| {_fmt_cad(asset_acb[asset])} "
            f"| {_fmt_cad(asset_fees[asset])} "
            f"| {_fmt_cad(asset_gain_loss[asset])} |"
        )

    return "\n".join(lines)


def _build_tp_part5_holdings(acb_final: list[ACBState]) -> str:
    if not acb_final:
        return "*No open positions at year end.*"

    lines = [
        "| Asset | Units Held | Total ACB (CAD) | ACB / Unit (CAD) |",
        "|-------|-----------|-----------------|-----------------|",
    ]
    for state in sorted(acb_final, key=lambda s: s.asset):
        lines.append(
            f"| {state.asset} "
            f"| {_fmt_units(state.total_units)} "
            f"| {_fmt_cad(state.total_acb_cad)} "
            f"| {_fmt_cad(state.acb_per_unit_cad)} |"
        )

    return "\n".join(lines)


def _build_tp_part6_income_summary(
    income_events: list[IncomeEvent],
    tax_year: int,
) -> str:
    """Summary table for Part 6 — one row per (asset, income_type). Used in the filing report."""
    if not income_events:
        return f"*No cryptoasset income recorded for {tax_year}.*"

    # Aggregate by (asset, income_type)
    totals: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}  # key → (units, fmv_cad)
    for e in income_events:
        key = (e.asset, e.income_type)
        units, fmv = totals.get(key, (Decimal("0"), Decimal("0")))
        totals[key] = (units + e.units, fmv + e.fmv_cad)

    total_fmv = sum((fmv for _, fmv in totals.values()), Decimal("0"))

    lines = [
        "| Asset | Income Type | Total Units | Total Income (CAD) |",
        "|-------|-------------|-------------|-------------------|",
    ]
    for asset, income_type in sorted(totals.keys()):
        units, fmv = totals[(asset, income_type)]
        lines.append(
            f"| {asset} " f"| {income_type} " f"| {_fmt_units(units)} " f"| {_fmt_cad(fmv)} |"
        )

    lines.append("")
    lines.append(f"**Total cryptoasset income: {_fmt_cad(total_fmv)} CAD**")
    lines.append("")
    lines.append("> Enter cryptoasset income on your T1 return (line 13000 for other income)")
    lines.append("> and on Schedule G / TP-1-V line 154 for Quebec property income.")

    return "\n".join(lines)


def _build_tp_part6_income_detail(
    income_events: list[IncomeEvent],
    tax_year: int,
) -> str:
    """Full per-event table for Part 6. Used in the detail backup file."""
    if not income_events:
        return f"*No cryptoasset income recorded for {tax_year}.*"

    lines = [
        "| Date | Asset | Units | Income Type | FMV (CAD) | Source |",
        "|------|-------|-------|-------------|-----------|--------|",
    ]

    sorted_events = sorted(income_events, key=lambda e: (e.timestamp, e.id))
    total_fmv = Decimal("0")
    for e in sorted_events:
        total_fmv += e.fmv_cad
        lines.append(
            f"| {_date_str(e.timestamp)} "
            f"| {e.asset} "
            f"| {_fmt_units(e.units)} "
            f"| {e.income_type} "
            f"| {_fmt_cad(e.fmv_cad)} "
            f"| {e.source} |"
        )

    lines.append("")
    lines.append(f"**Total cryptoasset income: {_fmt_cad(total_fmv)} CAD**")

    return "\n".join(lines)


def _build_schedule_g_summary(
    dispositions: list[AdjustedDisposition],
    inclusion_rate: Decimal,
) -> str:
    net = sum((d.adjusted_gain_loss_cad for d in dispositions), Decimal("0"))
    taxable = max(net, Decimal("0")) * inclusion_rate
    rate_str = _fmt_rate(inclusion_rate)

    lines = [
        "| | Amount (CAD) |",
        "|--|--:|",
        f"| Net capital gain (loss) | {_fmt_cad(net)} |",
        f"| **Taxable capital gains (\xd7{rate_str}) \u2192 TP-1-V Line 139**"
        f" | **{_fmt_cad(taxable)}** |",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def build_report(  # noqa: PLR0913 PLR0915
    dispositions: list[AdjustedDisposition],
    income_events: list[IncomeEvent],
    acb_final: list[ACBState],
    acquisitions: list[ClassifiedEvent],
    tax_year: int,
    inclusion_rate: Decimal,
    batch_name: str,
    sirop_version: str,
) -> str:
    """Build the filing summary Markdown report.

    Contains only what is needed to complete the tax forms — income is
    aggregated by asset type, not listed per event. For the full per-event
    transaction log use :func:`build_detail_report`.

    Pure function — no I/O, no DB access, no network calls.
    All inputs are typed dataclasses; returns the complete report as a string.
    """
    date_canada = _to_toronto(datetime.now(tz=ZoneInfo("UTC"))).strftime("%Y-%m-%d %H:%M %Z")

    sections: list[str] = []

    # -----------------------------------------------------------------------
    # Header
    # -----------------------------------------------------------------------
    sections.append(f"# sirop Tax Report \u2014 {batch_name} (Tax Year {tax_year})")
    sections.append("")
    sections.append(f"> Generated {date_canada} \xb7 sirop {sirop_version}")
    sections.append(">")
    sections.append(
        "> **This is a working document for your reference only. It is not a tax filing"
    )
    sections.append("> and does not constitute legal or tax advice. Verify all figures with a")
    sections.append("> qualified tax professional before submitting any return.**")
    sections.append("")
    sections.append("---")

    # -----------------------------------------------------------------------
    # How to use
    # -----------------------------------------------------------------------
    sections.append("")
    sections.append("## How to Use This Report")
    sections.append("")
    sections.append(
        "- **TurboTax Canada / UFile (federal):** Enter each row from Part A"
        ' \u2192 Schedule 3 \u2192 "Other properties".'
    )
    sections.append(
        "- **TurboTax Quebec / Imp\xf4tRapide (Quebec):** Enter on Schedule G"
        " using the same columns."
    )
    sections.append(
        "- **TP-21.4.39-V:** Complete this mandatory Quebec form using the"
        " grouped summaries in Part B."
    )
    sections.append(
        "- **T1 Line 12700:** Enter the taxable capital gains figure from the"
        " Part A summary box."
    )
    sections.append("- **TP-1-V Line 139:** Enter the same taxable capital gains figure.")
    sections.append("")
    sections.append("---")

    # -----------------------------------------------------------------------
    # Part A — Federal / CRA Schedule 3
    # -----------------------------------------------------------------------
    sections.append("")
    sections.append("## Part A \u2014 Federal \xb7 CRA Schedule 3 (Capital Gains and Losses)")
    sections.append("")
    sections.append("**Enter the net total on T1 Line 12700 (Taxable Capital Gains)**")
    sections.append("")
    sections.append("### Dispositions")
    sections.append("")
    sections.append(_build_dispositions_table(dispositions, tax_year))
    sections.append("")
    sections.append("### Summary")
    sections.append("")
    sections.append(_build_summary_table(dispositions, inclusion_rate))

    sections.append("")
    sections.append("---")

    # -----------------------------------------------------------------------
    # Part B — Quebec / TP-21.4.39-V and Schedule G
    # -----------------------------------------------------------------------
    sections.append("")
    sections.append("## Part B \u2014 Quebec \xb7 TP-21.4.39-V and Schedule G")

    # Part 3: Acquisitions
    sections.append("")
    sections.append("### TP-21.4.39-V \u2014 Part 3: Acquisitions During the Year")
    sections.append("")
    sections.append(_build_tp_part3_acquisitions(acquisitions, tax_year))

    # Part 4: Dispositions by asset
    sections.append("")
    sections.append("### TP-21.4.39-V \u2014 Part 4: Dispositions by Asset")
    sections.append("")
    sections.append(_build_tp_part4_dispositions(dispositions, tax_year))

    # Part 5: Year-end holdings
    sections.append("")
    sections.append("### TP-21.4.39-V \u2014 Part 5: Year-End Holdings")
    sections.append("")
    sections.append(_build_tp_part5_holdings(acb_final))

    # Part 6: Cryptoasset income (summary by asset — see detail file for per-event log)
    sections.append("")
    sections.append("### TP-21.4.39-V \u2014 Part 6: Cryptoasset Income")
    sections.append("")
    sections.append(_build_tp_part6_income_summary(income_events, tax_year))

    # Schedule G summary
    sections.append("")
    sections.append("### Schedule G Summary")
    sections.append("")
    sections.append("Quebec Schedule G uses the same capital gains figures as CRA Schedule 3.")
    sections.append("")
    sections.append(_build_schedule_g_summary(dispositions, inclusion_rate))
    sections.append("")
    sections.append("> **Quebec secondary impacts to review:**")
    sections.append(
        "> - **Schedule D (Solidarity Tax Credit):** High net income reduces the credit."
    )
    sections.append(
        "> - **Schedule K (RAMQ Drug Insurance Premium):** Maximum ~$755/year; income-tested."
    )
    sections.append(
        "> - **Schedule F (Health Services Fund \u2014 HSF):**"
        " ~1% contribution on investment income."
    )

    # TP-21.4.39-V mandatory filing reminder
    sections.append("")
    sections.append("### TP-21.4.39-V Mandatory Filing Reminder")
    sections.append("")
    sections.append(
        "> \u26a0 **Mandatory form:** Quebec requires all residents who held or transacted"
    )
    sections.append(
        f"> cryptocurrency during {tax_year} to complete form **TP-21.4.39-V** and check"
    )
    sections.append("> **TP-1-V Line 24**.")
    sections.append(">")
    sections.append("> **Penalty for late or missing filing:** $10/day, maximum $2,500.")
    sections.append("> An additional $100 penalty applies per omitted or erroneous item.")

    sections.append("")
    sections.append("---")
    sections.append("")
    sections.append(
        f"*Report generated by sirop {sirop_version}"
        f" \xb7 Data source: {batch_name}.sirop"
        f" \xb7 See {batch_name}-{tax_year}-tax-detail.md for full transaction log*"
    )

    return "\n".join(sections) + "\n"


def build_detail_report(  # noqa: PLR0913
    income_events: list[IncomeEvent],
    acb_final: list[ACBState],
    dispositions: list[AdjustedDisposition],
    tax_year: int,
    batch_name: str,
    sirop_version: str,
) -> str:
    """Build the audit-trail detail backup Markdown document.

    Contains the full per-event income log, year-end ACB pool, and
    superficial loss detail. Not needed for filing — keep as backup.

    Pure function — no I/O, no DB access, no network calls.
    """
    date_canada = _to_toronto(datetime.now(tz=ZoneInfo("UTC"))).strftime("%Y-%m-%d %H:%M %Z")

    sections: list[str] = []

    sections.append(f"# sirop Detail Backup \u2014 {batch_name} (Tax Year {tax_year})")
    sections.append("")
    sections.append(f"> Generated {date_canada} \xb7 sirop {sirop_version}")
    sections.append(">")
    sections.append("> Audit trail for your records. Not required for filing.")
    sections.append("")
    sections.append("---")

    # -----------------------------------------------------------------------
    # Income event log
    # -----------------------------------------------------------------------
    sections.append("")
    sections.append("## Cryptoasset Income \u2014 Full Event Log")
    sections.append("")
    sections.append(
        "Every individual income event (staking rewards, interest, airdrops, etc.)."
        " The totals match Part 6 of the filing summary."
    )
    sections.append("")
    sections.append(_build_tp_part6_income_detail(income_events, tax_year))

    sections.append("")
    sections.append("---")

    # -----------------------------------------------------------------------
    # Superficial losses detail (only if any)
    # -----------------------------------------------------------------------
    sl_section = _build_superficial_losses_section(dispositions)
    if sl_section:
        sections.append("")
        sections.append("## Superficial Loss Detail")
        sections.append("")
        sections.append(sl_section)
        sections.append("")
        sections.append("---")

    # -----------------------------------------------------------------------
    # Year-end ACB pool
    # -----------------------------------------------------------------------
    sections.append("")
    sections.append("## Year-End ACB Pool")
    sections.append("")
    sections.append("Carry-forward cost basis for next tax year.")
    sections.append("")
    sections.append(_build_tp_part5_holdings(acb_final))

    sections.append("")
    sections.append("---")
    sections.append("")
    sections.append(
        f"*Detail backup generated by sirop {sirop_version}"
        f" \xb7 Data source: {batch_name}.sirop*"
    )

    return "\n".join(sections) + "\n"
