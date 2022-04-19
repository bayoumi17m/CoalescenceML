from typing import Optional

import click
from rich.markdown import Markdown

from coalescenceml.cli import utils as cli_utils
from coalescenceml.cli.cli import cli
from coalescenceml.config.base_config import BaseConfiguration
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.config.profile_config import (
    ProfileConfiguration,
    get_default_store_type,
)
from coalescenceml.constants import console
from coalescenceml.directory import Directory
from coalescenceml.enums import DirectoryStoreFlavor, LoggingLevels


# Logging
@cli.group()
def logging() -> None:
    """Configuration of logging for ZenML pipelines."""


# Setting logging
@logging.command("set-verbosity")
@click.argument(
    "verbosity",
    type=click.Choice(
        list(map(lambda x: x.name, LoggingLevels)), case_sensitive=False
    ),
)
def set_logging_verbosity(verbosity: str) -> None:
    """Set logging level"""
    verbosity = verbosity.upper()
    if verbosity not in LoggingLevels.__members__:
        raise KeyError(
            f"Verbosity must be one of {list(LoggingLevels.__members__.keys())}"
        )
    cli_utils.info(f"Set verbosity to: {verbosity}")


# Profiles
@cli.group()
def profile() -> None:
    """Configuration of ZenML profiles."""


@profile.command("create")
@click.argument(
    "name",
    type=str,
    required=True,
)
@click.option(
    "--url",
    "-u",
    "url",
    help="The store URL to use for the profile.",
    required=False,
    type=str,
)
@click.option(
    "--store-type",
    "-t",
    "store_type",
    help="The store type to use for the profile.",
    required=False,
    type=click.Choice(list(DirectoryStoreFlavor)),
    default=get_default_store_type(),
)
def create_profile_command(
    name: str, url: Optional[str], store_type: Optional[DirectoryStoreFlavor]
) -> None:
    """Create a new configuration profile."""

    cli_utils.print_active_profile()

    cfg = GlobalConfiguration()

    if cfg.get_profile(name):
        cli_utils.error(f"Profile {name} already exists.")
        return
    cfg.add_or_update_profile(
        ProfileConfiguration(name=name, store_url=url, store_type=store_type)
    )
    cli_utils.info(f"Profile '{name}' successfully created.")


@profile.command("list")
def list_profiles_command() -> None:
    """List configuration profiles."""

    cli_utils.print_active_profile()

    cfg = GlobalConfiguration()
    dir_ = Directory()

    profiles = cfg.profiles

    if len(profiles) == 0:
        cli_utils.warning("No profiles configured!")
        return

    profile_dicts = []
    for profile_name, profile in profiles.items():
        is_active = profile_name == dir_.active_profile_name
        profile_config = {
            "ACTIVE": ":point_right:" if is_active else "",
            "PROFILE NAME": profile_name,
            "STORE TYPE": profile.store_type.value,
            "URL": profile.store_url,
            "ACTIVE STACK": dir_.active_stack_name,
        }
        profile_dicts.append(profile_config)

    cli_utils.print_table(profile_dicts)


@profile.command(
    "describe",
    help="Show details about the active profile.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=False,
)
def describe_profile(name: Optional[str]) -> None:
    """Show details about a named profile or the active profile."""
    cli_utils.print_active_profile()

    dir_ = Directory()
    name = name or dir_.active_profile_name

    profile = GlobalConfiguration().get_profile(name)
    if not profile:
        cli_utils.error(f"Profile '{name}' does not exist.")
        return

    cli_utils.print_profile(
        profile,
        active=name == dir_.active_profile_name,
    )


@profile.command("delete")
@click.argument("name", type=str)
def delete_profile(name: str) -> None:
    """Delete a profile.
    If the profile is currently active, it cannot be deleted.
    """
    cli_utils.print_active_profile()

    with console.status(f"Deleting profile '{name}'...\n"):

        cfg = GlobalConfiguration()
        dir_ = Directory()
        if not cfg.get_profile(name):
            cli_utils.error(f"Profile {name} doesn't exist.")
            return
        if cfg.active_profile_name == name:
            cli_utils.error(
                f"Profile '{name}' cannot be deleted because it's globally "
                f" active. Please choose a different active global profile "
                f"first by running 'coml profile set --global PROFILE'."
            )
            return

        if dir_.active_profile_name == name:
            cli_utils.error(
                f"Profile '{name}' cannot be deleted because it's locally "
                f"active. Please choose a different active profile first by "
                f"running 'coml profile set PROFILE'."
            )
            return

        cfg.delete_profile(name)
        cli_utils.info(f"Deleted profile '{name}'.")


@profile.command("set")
@click.argument("name", type=str)
@click.option(
    "--global",
    "-g",
    "global_profile",
    is_flag=True,
    help="Set the global active profile",
)
def set_active_profile(name: str, global_profile: bool = False) -> None:
    """Set a profile as active.
    If the '--global' flag is set, the profile will be set as the global
    active profile, otherwise as the directory local active profile.
    """
    cli_utils.print_active_profile()
    scope = " global" if global_profile else ""

    with console.status(f"Setting the{scope} active profile to '{name}'..."):

        cfg: BaseConfiguration = (
            GlobalConfiguration() if global_profile else Directory()
        )

        current_profile_name = cfg.active_profile_name
        if current_profile_name == name:
            cli_utils.info(f"Profile '{name}' is already{scope}ly active.")
            return

        try:
            cfg.activate_profile(name)
        except Exception as e:
            cli_utils.error(f"Error activating{scope} profile: {str(e)}. ")
            if current_profile_name:
                cli_utils.info(
                    f"Keeping current{scope} profile: '{current_profile_name}'."
                )
                cfg.activate_profile(current_profile_name)
            return
        cli_utils.info(f"Active{scope} profile changed to: '{name}'")


@profile.command("get")
def get_active_profile() -> None:
    """Get the active profile."""
    with console.status("Getting the active profile..."):
        cli_utils.info(f"Active profile is: {Directory().active_profile_name}")


@profile.command("explain")
def explain_profile() -> None:
    """Explains the concept of CoalescenceML profiles."""

    with console.pager():
        console.print(
            Markdown(
                """
TODO: Explain in words and with examples about what a profile is and the CLI
command. This should showcase one of every command probably and the most common
methods that they will want to use.
"""
            )
        )
