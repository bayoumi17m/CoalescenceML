from typing import Optional, Tuple

import click
from rich.markdown import Markdown
from rich.progress import track

from coalescenceml.cli.cli import cli
from coalescenceml.cli.utils import (
    confirmation,
    error,
    format_integration_list,
    info,
    install_packages,
    print_table,
    title,
    uninstall_package,
    warning,
)
from coalescenceml.constants import console
from coalescenceml.integrations.registry import integration_registry
from coalescenceml.logger import get_logger


logger = get_logger(__name__)


@cli.group(help="Interact with the requirements of external integrations.")
def integration() -> None:
    """Integrations group"""


@integration.command(name="list", help="List the available integrations.")
def list_integrations() -> None:
    """List all available integrations with their installation status."""
    formatted_table = format_integration_list(
        [x for x in integration_registry.integrations.items()]
    )
    print_table(formatted_table)
    warning(
        "\n" + "To install the dependencies of a specific integration, type: "
    )
    warning("coml integration install EXAMPLE_NAME")


@integration.command(
    name="requirements", help="List all requirements for an integration."
)
@click.argument("integration_name", required=False, default=None)
def get_requirements(integration_name: Optional[str] = None) -> None:
    """List all requirements for the chosen integration."""
    try:
        requirements = integration_registry.select_integration_requirements(
            integration_name
        )
    except KeyError as e:
        error(str(e))
    else:
        if requirements:
            title(
                f'Requirements for {integration_name or "all integrations"}:\n'
            )
            info(f"{requirements}")
            warning(
                "\n" + "To install the dependencies of a "
                "specific integration, type: "
            )
            warning("coml integration install EXAMPLE_NAME")


# TODO: How do we handle system requirements?
@integration.command(
    help="Install the required packages for the integration of choice."
)
@click.argument("integrations", nargs=-1, required=False)
@click.option(
    "--ignore-integration",
    "-i",
    multiple=True,
    help="List of integrations to ignore explicitly.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force the installation of the required packages. This will skip the "
    "confirmation step and reinstall existing packages as well",
)
def install(
    integrations: Tuple[str],
    ignore_integration: Tuple[str],
    force: bool = False,
) -> None:
    """Installs the required packages for a given integration. If no integration
    is specified all required packages for all integrations are installed
    using pip"""
    if not integrations:
        # no integrations specified, use all registered integrations
        integrations = set(integration_registry.integrations.keys())
        for i in ignore_integration:
            try:
                integrations.remove(i)
            except KeyError:
                error(
                    f"Integration {i} does not exist. Available integrations: "
                    f"{list(integration_registry.integrations.keys())}"
                )

    requirements = []
    integrations_to_install = []
    for integration_name in integrations:
        try:
            if force or not integration_registry.is_installed(integration_name):
                requirements += (
                    integration_registry.select_integration_requirements(
                        integration_name
                    )
                )
                integrations_to_install.append(integration_name)
            else:
                info(
                    f"All required packages for integration "
                    f"'{integration_name}' are already installed."
                )
        except KeyError:
            warning(f"Unable to find integration '{integration_name}'.")

    if requirements and (
        force
        or confirmation(
            "Are you sure you want to install the following "
            "packages to the current environment?\n"
            f"{requirements}"
        )
    ):
        with console.status("Installing integrations..."):
            install_packages(requirements)


@integration.command(
    help="Uninstall the required packages for the integration of choice."
)
@click.argument("integrations", nargs=-1, required=False)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force the uninstallation of the required packages. This will skip "
    "the confirmation step",
)
def uninstall(integrations: Tuple[str], force: bool = False) -> None:
    """Installs the required packages for a given integration. If no integration
    is specified all required packages for all integrations are installed
    using pip"""
    if not integrations:
        # no integrations specified, use all registered integrations
        integrations = tuple(integration_registry.integrations.keys())

    requirements = []
    for integration_name in integrations:
        try:
            if integration_registry.is_installed(integration_name):
                requirements += (
                    integration_registry.select_integration_requirements(
                        integration_name
                    )
                )
            else:
                warning(
                    f"Requirements for integration '{integration_name}' "
                    f"already not installed."
                )
        except KeyError:
            warning(f"Unable to find integration '{integration_name}'.")

    if requirements and (
        force
        or confirmation(
            "Are you sure you want to uninstall the following "
            "packages from the current environment?\n"
            f"{requirements}"
        )
    ):
        for n in track(
            range(len(requirements)),
            description="Uninstalling integrations...",
        ):
            uninstall_package(requirements[n])


@integration.command("explain")
def explain_integrations() -> None:
    """Explains the concept of CoalescenceML integrations."""

    with console.pager():
        console.print(
            Markdown(
                """
TODO: Explain in words and with examples about what an integration is and the CLI
command. This should showcase one of every command probably and the most common
methods that they will want to use.
"""
            )
        )
