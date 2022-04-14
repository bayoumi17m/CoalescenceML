import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import click
from rich import box, table
from rich.text import Text

from coalescenceml.config.profile_config import ProfileConfiguration
from coalescenceml.logger import get_logger
from coalescenceml.constants import console, IS_DEBUG_ENV
from coalescenceml.directory import Directory
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.integrations.integration import Integration
from coalescenceml.stack import StackComponent


logger = get_logger(__name__)


def title(text: str) -> None:
    """Echo a title formatted string on the CLI.

    Args:
        text: Input text string
    """
    console.print(text.upper(), style="title")
    #click.echo(click.style(text.upper(), fg="cyan", bold=True, underline=True))


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
        ClickException: when called.
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
    console.print(text, style="info")
    #click.echo(click.style(text, fg="green"))


def print_table(tab: List[Dict[str, Any]]) -> None:
    """Prints the list of dicts in a table format.

    ..note: Each item in the list represents a line in the table. Each dict
    should have the same keys.

    ..warning: Undefined behavior if list is empty!

    Args:
        tab: A table represented as a list of dicts
    """
    columns = {key.upper(): None for key in tab[0].keys()}
    rich_table = table.Table(*columns.keys(), box=box.HEAVY_EDGE)

    for dict_ in tab:
        values = columns.copy()
        values.update(dict_)
        rich_table.add_row(*list(values.values()))

    if len(rich_table.columns) > 1:
        rich_table.columns[0].justify = "center"
    console.print(rich_table)


def format_integration_list(
    integrations: List[Tuple[str, Integration]]
) -> List[Dict[str, str]]:
    """Formats a list of integrations into a List of Dicts."""
    
    list_of_dicts = []
    for name, integration_impl in integrations:
        is_installed = integration_impl.check_installation()
        list_of_dicts.append(
            {
                "INSTALLED": ":white_check_mark:" if is_installed else "",
                "INTEGRATION": name,
                "REQUIRED_PACKAGES": ", ".join(integration_impl.REQUIREMENTS),
            }
        )
    return list_of_dicts


def print_stack_component_list(
    components: List[StackComponent],
    active_component_name: Optional[str] = None,
) -> None:
    """Prints a table with configuration options for a list of stack components.
    
    ..note: If a component is active (its name matches the
    `active_component_name`), it will be highlighted in a separate table column.
    
    Args:
        components: List of stack components to print.
        active_component_name: Name of the component that is currently
            active.
    """
    configurations = []
    for component in components:
        is_active = component.name == active_component_name
        component_config = {
            "ACTIVE": ":point_right:" if is_active else "",
            **{
                key.upper(): str(value)
                for key, value in component.dict().items()
            },
        }
        configurations.append(component_config)
    print_table(configurations)


def print_stack_configuration(
    config: Dict[StackComponentFlavor, str], active: bool, stack_name: str
) -> None:
    """Prints the configuration options of a stack.
    
    Args:
        config:
        active:
        stack_name:
    """
    stack_caption = f"'{stack_name}' stack"
    if active:
        stack_caption += " (ACTIVE)"
    rich_table = table.Table(
        box=box.HEAVY_EDGE,
        title="Stack Configuration",
        caption=stack_caption,
        show_lines=True,
    )
    rich_table.add_column("COMPONENT_TYPE")
    rich_table.add_column("COMPONENT_NAME")
    for component_type, name in config.items():
        rich_table.add_row(component_type.value, name)

    # capitalize entries in first column
    rich_table.columns[0]._cells = [
        component.upper() for component in rich_table.columns[0]._cells
    ]
    console.print(rich_table)


def print_stack_component_configuration(
    component: StackComponent, display_name: str, active_status: bool
) -> None:
    """Prints the configuration options of a stack component."""
    title = f"{component.TYPE.value.upper()} Component Configuration"
    if active_status:
        title += " (ACTIVE)"
    rich_table = table.Table(
        box=box.HEAVY_EDGE,
        title=title,
        show_lines=True,
    )
    rich_table.add_column("COMPONENT_PROPERTY")
    rich_table.add_column("VALUE")
    items = component.dict().items()
    for item in items:
        rich_table.add_row(*[str(elem) for elem in item])

    # capitalize entries in first column
    rich_table.columns[0]._cells = [
        component.upper() for component in rich_table.columns[0]._cells
    ]
    console.print(rich_table)


def print_active_profile() -> None:
    """Print active profile."""
    dir_ = Directory()
    scope = "local" if dir_.root else "global"
    info(
        f"Running with active profile: '{dir_.active_profile_name}' ({scope})"
    )


def print_active_stack() -> None:
    """Print active stack."""
    dir_ = Directory()
    info(f"Running with active stack: '{dir_.active_stack_name}'")


def print_profile(
    profile: ProfileConfiguration,
    active: bool,
) -> None:
    """Prints the configuration options of a profile.
    Args:
        profile: Profile to print.
        active: Whether the profile is active.
        name: Name of the profile.
    """
    profile_title = f"'{profile.name}' Profile Configuration"
    if active:
        profile_title += " (ACTIVE)"

    rich_table = table.Table(
        box=box.HEAVY_EDGE,
        title=profile_title,
        show_lines=True,
    )
    rich_table.add_column("PROPERTY")
    rich_table.add_column("VALUE")
    items = profile.dict().items()
    for item in items:
        rich_table.add_row(*[str(elem) for elem in item])

    # capitalize entries in first column
    rich_table.columns[0]._cells = [
        component.upper() for component in rich_table.columns[0]._cells
    ]
    console.print(rich_table)


def parse_unknown_options(args: List[str]) -> Dict[str, Any]:
    """Parse unknown options from the CLI.
    Args:
      args: A list of strings from the CLI.
    Returns:
        Dict of parsed args.
    """
    warning_message = (
        "Please provide args with a proper "
        "identifier as the key and the following structure: "
        '--custom_argument="value"'
    )

    assert all(a.startswith("--") for a in args), warning_message
    assert all(len(a.split("=")) == 2 for a in args), warning_message

    p_args = [a.lstrip("--").split("=") for a in args]

    assert all(k.isidentifier() for k, _ in p_args), warning_message

    r_args = {k: v for k, v in p_args}
    assert len(p_args) == len(r_args), "Replicated arguments!"

    return r_args


def install_packages(package: str) -> None:
    """Installs pypi package into the current environment with pip.

    Args:
        package: pypi package to install
    """
    # TODO: Can we utilize a package manager instead? Such as their package
    # manager so that we don't create conflicts?
    # IF we enforce the usage of `poetry` then this is do-able.
    # subprocess.check_call(["poetry", "add", package])
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def uninstall_package(package: str) -> None:
    """Uninstalls pypi package from the current environment with pip.

    Args:
        package: pypi package to install
    """
    # TODO: Can we utilize a package manager instead? Such as their package
    # manager so that we don't create conflicts?
    # IF we enforce the usage of `poetry` then this is do-able.
    # subprocess.check_call(["poetry", "remove", package])
    subprocess.check_call(
        [sys.executable, "-m", "pip", "uninstall", "-y", package]
    )
