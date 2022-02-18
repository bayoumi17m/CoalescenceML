from click.core import Group

from coalescenceml.cli import cli


def test_cli_command_defines_a_cli_group() -> None:
    """Check that cli command defines a CLI group when invoked."""
    assert isinstance(cli, Group)
