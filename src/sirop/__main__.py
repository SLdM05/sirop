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

from sirop.cli.create import handle_create
from sirop.cli.list_batches import handle_list
from sirop.cli.switch import handle_switch
from sirop.cli.tap import handle_tap
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
            "Exchange format to use (e.g. ndax, shakepay). "
            "Auto-detected from headers when omitted."
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

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    configure_logging(verbose=args.verbose, debug=args.debug)

    if args.command == "tap":
        sys.exit(handle_tap(args.file, args.source))

    elif args.command == "create":
        sys.exit(handle_create(args.name, args.year))

    elif args.command == "list":
        sys.exit(handle_list())

    elif args.command == "switch":
        sys.exit(handle_switch(args.name))

    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
