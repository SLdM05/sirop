"""
sirop — Quebec maple syrup-themed crypto tax tool.

Entry point for the `sirop` CLI. Wires argparse subcommands to their
handlers and initialises logging before dispatching.

All subcommand handlers live in sirop.cli.*. This module contains only
argument parsing and dispatch — no business logic.
"""

import argparse
import sys
from pathlib import Path

from sirop.cli.boil import handle_boil
from sirop.cli.create import handle_create
from sirop.cli.list_batches import handle_list
from sirop.cli.stir import handle_stir
from sirop.cli.switch import handle_switch
from sirop.cli.tap import handle_tap, handle_tap_walletfolder
from sirop.utils.logging import configure_logging


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sirop",
        description="Quebec crypto tax calculator — boil your gains, grade your season.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show sensitive values in log output"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Set log level to DEBUG (implies --verbose)"
    )

    sub = parser.add_subparsers(dest="command", metavar="<command>")

    # ── tap ───────────────────────────────────────────────────────────────────
    tap_p = sub.add_parser("tap", help="Import a CSV exchange export into the active batch")
    tap_p.add_argument("file", type=Path, help="Path to the exchange CSV export")
    tap_p.add_argument(
        "--source",
        metavar="NAME",
        help=(
            "Exchange format to use (e.g. ndax, shakepay). Auto-detected from headers when omitted."
        ),
    )
    tap_p.add_argument(
        "--wallet",
        metavar="NAME",
        help=(
            "Wallet name to assign these transactions to (e.g. 'ledger-cold', 'shakepay-savings'). "
            "Defaults to the detected source format name. Use this to distinguish two accounts "
            "at the same exchange, or to label a hardware wallet before tapping its CSV."
        ),
    )
    tap_p.add_argument(
        "--walletfolder",
        "--wf",
        action="store_true",
        dest="walletfolder",
        default=False,
        help=(
            "Treat each immediate subfolder of the given directory as a separate wallet. "
            "CSV files in each subfolder are imported under a wallet named after that subfolder. "
            "Example: tap data/ --wf  imports data/ledger-cold/*.csv as wallet 'ledger-cold'."
        ),
    )

    # ── create ────────────────────────────────────────────────────────────────
    create_p = sub.add_parser("create", help="Tap a new batch (tax year file)")
    create_p.add_argument("name", help="Batch name, e.g. my2025tax")
    create_p.add_argument(
        "--year",
        type=int,
        metavar="YYYY",
        help="Tax year (inferred from name if omitted)",
    )

    # ── list ──────────────────────────────────────────────────────────────────
    sub.add_parser("list", help="List all batches in DATA_DIR")

    # ── switch ────────────────────────────────────────────────────────────────
    switch_p = sub.add_parser("switch", help="Set the active batch")
    switch_p.add_argument("name", help="Batch name to activate")

    # ── stir ──────────────────────────────────────────────────────────────────
    stir_p = sub.add_parser(
        "stir",
        help=(
            "Review imported transactions, inspect auto-detected transfer pairs, "
            "and manually link or unlink pairs before boil."
        ),
    )
    stir_p.add_argument(
        "--list",
        action="store_true",
        dest="list_only",
        help="Show current transfer match state and exit (no interactive prompt).",
    )
    stir_p.add_argument(
        "--link",
        nargs=2,
        type=int,
        metavar=("ID1", "ID2"),
        help="Force two transactions to be treated as a transfer pair.",
    )
    stir_p.add_argument(
        "--unlink",
        nargs=2,
        type=int,
        metavar=("ID1", "ID2"),
        help="Prevent two transactions from being auto-paired.",
    )
    stir_p.add_argument(
        "--clear",
        type=int,
        metavar="ID",
        help="Remove all stir overrides for a transaction.",
    )

    # ── boil ──────────────────────────────────────────────────────────────────
    boil_p = sub.add_parser(
        "boil",
        help="Run the tax calculation pipeline (normalize → ACB → superficial loss)",
    )
    boil_p.add_argument(
        "--from",
        dest="from_stage",
        metavar="STAGE",
        choices=["normalize", "verify", "transfer_match", "boil", "superficial_loss"],
        default=None,
        help=(
            "Re-run from this stage onward, invalidating downstream stages. "
            "Choices: normalize, verify, transfer_match, boil, superficial_loss"
        ),
    )
    boil_p.add_argument(
        "--audit",
        action="store_true",
        default=False,
        help=(
            "After calculation, write a CSV ledger of all events and ACB math "
            "to <DATA_DIR>/<batch>-audit.csv for manual verification in Excel."
        ),
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    configure_logging(verbose=args.verbose, debug=args.debug)

    if args.command == "tap":
        if args.walletfolder:
            sys.exit(handle_tap_walletfolder(args.file, args.source))
        sys.exit(handle_tap(args.file, args.source, getattr(args, "wallet", None)))

    elif args.command == "create":
        sys.exit(handle_create(args.name, args.year))

    elif args.command == "list":
        sys.exit(handle_list())

    elif args.command == "switch":
        sys.exit(handle_switch(args.name))

    elif args.command == "stir":
        link: tuple[int, int] | None = (args.link[0], args.link[1]) if args.link else None
        unlink: tuple[int, int] | None = (args.unlink[0], args.unlink[1]) if args.unlink else None
        sys.exit(
            handle_stir(
                list_only=args.list_only,
                link=link,
                unlink=unlink,
                clear=args.clear,
            )
        )

    elif args.command == "boil":
        sys.exit(handle_boil(args.from_stage, audit=args.audit))

    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
