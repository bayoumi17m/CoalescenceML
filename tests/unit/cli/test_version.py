from click.testing import CliRunner

from coalescenceml import __version__ as current_coml_version
from coalescenceml.cli.version import version


def test_version_outputs() -> None:
    """Checks that CLI version command works and output right version."""
    runner = CliRunner()
    result = runner.invoke(version)
    assert result.exit_code == 0
    assert current_coml_version in result.output
