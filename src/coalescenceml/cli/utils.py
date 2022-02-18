import datetime
import functools
import subprocess
import sys
from typing import Any, Callable, Dict, List, TypeVar, cast

import click
from dateutil import tz
from tabulate import tabulate

from coalescenceml.logger import get_logger


logger = get_logger(__name__)


def title(text: str) -> None:
    """Echo a title formatted string on the CLI.

    Args:
      text: Input text string
    """
    click.echo(click.style(text.upper(), fg="cyan", bold=True, underline=True))


def confirmation(text: str, *args: Any, **kwargs: Any) -> bool:
    """Echo a confirmation string on the CLI.

    Args:
      text: Input text string.
      *args: Args to be passed to click.confirm()
      **kwargs: Kwargs to be passed to click.confirm()

    Returns:
        Boolean based on user response
    """
    return click.confirm(click.style(text, fg="yellow"), *args, **kwargs)


def error(text: str) -> None:
    """Echo an error string on the CLI.

    Args:
      text: Input text string

    Raises:
      click.ClickException: when called.
    """
    raise click.ClickException(message=click.style(text, fg="red", bold=True))


def warning(text: str) -> None:
    """Echo a warning string on the CLI.

    Args:
      text: Input text string.
    """
    click.echo(click.style(text, fg="yellow", bold=True))


def info(text: str) -> None:
    """Echo an info dump on the CLI.

    Args:
      text: Input text string.
    """
    click.echo(click.style(text, fg="green"))


def install_package(package: str) -> None:
    """Installs pypi package into the current environment with pip
    
    Args:
      package: pypi package to install
    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def uninstall_package(package: str) -> None:
    """Uninstalls pypi package from the current environment with pip
    
    Args:
      package: pypi package to install
    """
    subprocess.check_call(
        [sys.executable, "-m", "pip", "uninstall", "-y", package]
    )
