import click

from coalescenceml import __version__
from coalescenceml.logger import set_root_verbosity


@click.group()
@click.version_option(__version__, "--version", "-v")
def cli() -> None:
    """Coalescence ML CLI."""
    set_root_verbosity()


if __name__ == "__main__":
    cli()
