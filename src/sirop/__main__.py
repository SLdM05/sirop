"""sirop — Quebec maple syrup-themed crypto tax tool.

Entry point for the ``sirop`` CLI. Wires Click subcommands to their handlers
and initialises logging before dispatching.

All subcommand handlers live in :mod:`sirop.cli.*`. This module contains only
the top-level :class:`click.Group` and dispatch — no business logic.
"""

import rich_click as click

from sirop.cli.boil import boil_command
from sirop.cli.create import create_command
from sirop.cli.list_batches import list_command
from sirop.cli.pour import pour_command
from sirop.cli.stir import stir_command
from sirop.cli.switch import switch_command
from sirop.cli.tap import tap_command
from sirop.utils.logging import configure_logging

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = False
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = False
click.rich_click.STYLE_OPTION = "bold cyan"
click.rich_click.STYLE_ARGUMENT = "bold cyan"
click.rich_click.STYLE_COMMAND = "bold green"
click.rich_click.STYLE_SWITCH = "bold green"
click.rich_click.STYLE_HEADER_TEXT = "bold"


_EPILOG = (
    "Tip: if you have an existing .sirop file but no active batch set, "
    "run `sirop switch <name>` (where <name> is the filename without .sirop) "
    "to make it active."
)


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Quebec crypto tax calculator — boil your gains, grade your season.",
    epilog=_EPILOG,
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Show sensitive values in log output.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Set log level to DEBUG (implies --verbose).",
)
def cli(verbose: bool, debug: bool) -> None:
    """Top-level ``sirop`` command group."""
    configure_logging(verbose=verbose, debug=debug)


cli.add_command(tap_command)
cli.add_command(create_command)
cli.add_command(list_command)
cli.add_command(switch_command)
cli.add_command(stir_command)
cli.add_command(boil_command)
cli.add_command(pour_command)


def main() -> None:
    """Console-script entry point declared in ``pyproject.toml``."""
    cli()


if __name__ == "__main__":
    main()
