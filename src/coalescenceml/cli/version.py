import random

import click

from coalescenceml import __version__
from coalescenceml.cli.cli import cli
from coalescenceml.cli.utils import info

ascii_arts = {
    "Alpha": "CoML"
}


@cli.command()
def version() -> None:
    """Version of CoalescenceML
    """
    info(ascii_arts["Alpha"])
    click.echo(click.style(f"version: {__version__}", bold=True))
