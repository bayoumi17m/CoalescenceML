import click

from coalesenceml import __version__
# from coalesenceml.logger import set_root_verbosity


@click.group()
@click.version_option(__version__, "--version", "-v")
def cli() -> None:
    """CoML CLI"""


if __name__ == "__main__":
    cli()
