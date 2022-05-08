import time
from importlib import import_module
from typing import Any, Callable, Dict, List, Optional

import click
from rich.markdown import Markdown

from coalescenceml.cli import utils as cli_utils
from coalescenceml.cli.cli import cli
from coalescenceml.constants import console
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.io import fileio
from coalescenceml.directory import Directory
from coalescenceml.stack import StackComponent


def component_display_name_helper(
    component_type: StackComponentFlavor
) -> str:
    """Human-readable name for a stack component."""
    name = component_type.value
    return name.replace("_", " ")


def _get_stack_component(
    component_type: StackComponentFlavor,
    component_name: Optional[str] = None,
) -> StackComponent:
    """Gets a stack component for a given type and name.

    Args:
        component_type: Type of the component to get.
        component_name: Name of the component to get. If `None`, the
            component of the active stack gets returned.

    Returns:
        A stack component of the given type.

    Raises:
        KeyError: If no stack component is registered for the given name.
    """
    dir_ = Directory()
    if component_name:
        return dir_.get_stack_component(component_type, name=component_name)

    component = dir_.active_stack.components[component_type]
    cli_utils.info(
        f"No component name given, using `{component.name}` "
        f"from active stack."
    )
    return component

def register_stack_component_helper(
    component_type: StackComponentFlavor,
    component_name: str,
    component_flavor: str,
    **kwargs: Dict[str, Any],
) -> None:
    """Register a stack component helper."""
    from coalescenceml.stack.stack_component_class_registry import (
        StackComponentClassRegistry,
    )

    component_cls = StackComponentClassRegistry.get_class(
        component_type=component_type,
        component_flavor=component_flavor,
    )

    component = component_cls(name=component_name, **kwargs)
    Directory().register_stack_component(component)


def generate_stack_component_get_command(
    component_type: StackComponentFlavor,
) -> Callable[[], None]:
    """Generates a `get` command for the specific stack component type."""

    def get_stack_component_command() -> None:
        """Prints the name of the active component."""

        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        active_stack = Directory().active_stack
        component = active_stack.components.get(component_type, None)
        display_name = component_display_name_helper(component_type)
        if component:
            cli_utils.info(f"Active {display_name}: '{component.name}'")
        else:
            cli_utils.warning(
                f"No {display_name} set for active stack ('{active_stack.name}')."
            )

    return get_stack_component_command


def generate_stack_component_describe_command(
    component_type: StackComponentFlavor,
) -> Callable[[Optional[str]], None]:
    """Generates a `describe` command for the specific stack component type."""

    @click.argument(
        "name",
        type=str,
        required=False,
    )
    def describe_stack_component_command(name: Optional[str]) -> None:
        """Prints details about the active/specified component."""
        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        display_name = component_display_name_helper(component_type)
        dir_ = Directory()
        components = dir_.get_stack_components(component_type)
        if len(components) == 0:
            cli_utils.warning(f"No {display_name} registered.")
            return

        try:
            component = _get_stack_component(
                component_type, component_name=name
            )
        except KeyError:
            if name:
                cli_utils.warning(
                    f"No {display_name} found for name '{name}'."
                )
            else:
                cli_utils.warning(
                    f"No {display_name} in active stack."
                )
            return

        try:
            active_component_name = dir_.active_stack.components[
                component_type
            ].name
            is_active = active_component_name == component.name
        except KeyError:
            # there is no component of this type in the active stack
            is_active = False

        cli_utils.print_stack_component_configuration(
            component, display_name, is_active
        )

    return describe_stack_component_command


def generate_stack_component_list_command(
    component_type: StackComponentFlavor,
) -> Callable[[], None]:
    """Generates a `list` command for the specific stack component type."""

    def list_stack_components_command() -> None:
        """Prints a table of stack components."""

        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        dir_ = Directory()

        components = dir_.get_stack_components(component_type)
        display_name = component_display_name_helper(component_type)
        if len(components) == 0:
            cli_utils.warning(f"No {display_name} registered.")
            return
        try:
            active_component_name = dir_.active_stack.components[
                component_type
            ].name
        except KeyError:
            active_component_name = None

        cli_utils.print_stack_component_list(
            components, active_component_name=active_component_name
        )

    return list_stack_components_command


def generate_stack_component_register_command(
    component_type: StackComponentFlavor,
) -> Callable[[str, str, List[str]], None]:
    """Generates a `register` command for the specific stack component type."""
    display_name = component_display_name_helper(component_type)

    @click.argument(
        "name",
        type=str,
        required=True,
    )
    @click.option(
        "--type",
        "-t",
        "flavor",
        help=f"The type of the {display_name} to register.",
        required=True,
        type=str,
    )
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    def register_stack_component_command(
        name: str, flavor: str, args: List[str]
    ) -> None:
        """Registers a stack component."""
        cli_utils.print_active_profile()
        try:
            parsed_args = cli_utils.parse_unknown_options(args)
        except AssertionError as e:
            cli_utils.error(str(e))
            return


        register_stack_component_helper(
            component_type=component_type,
            component_name=component_name,
            component_flavor=component_flavor,
            **parsed_args,
        )

        cli_utils.info(f"Successfully registered {display_name} `{name}`.")

    return register_stack_component_command


def generate_stack_component_delete_command(
    component_type: StackComponentFlavor,
) -> Callable[[str], None]:
    """Generates a `delete` command for the specific stack component type."""

    @click.argument("name", type=str)
    def delete_stack_component_command(name: str) -> None:
        """Deletes a stack component."""
        cli_utils.print_active_profile()
        Directory().deregister_stack_component(
            component_type=component_type,
            name=name,
        )
        display_name = component_display_name_helper(component_type)
        cli_utils.info(f"Deleted {display_name}: {name}")

    return delete_stack_component_command


def generate_stack_component_up_command(
    component_type: StackComponentFlavor,
) -> Callable[[Optional[str]], None]:
    """Generates a `up` command for the specific stack component type."""

    @click.argument("name", type=str, required=False)
    def up_stack_component_command(name: Optional[str] = None) -> None:
        """Deploys a stack component locally."""
        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        component = _get_stack_component(component_type, component_name=name)
        display_name = component_display_name_helper(component_type)

        if component.is_running:
            cli_utils.info(
                f"Local deployment is already running for {display_name} "
                f"'{component.name}'."
            )
            return

        if not component.is_provisioned:
            cli_utils.info(
                f"Provisioning local resources for {display_name} "
                f"'{component.name}'."
            )
            try:
                component.provision()
            except NotImplementedError:
                cli_utils.error(
                    f"Provisioning local resources not implemented for "
                    f"{display_name} '{component.name}'."
                )

        if not component.is_running:
            cli_utils.info(
                f"Resuming local resources for {display_name} "
                f"'{component.name}'."
            )
            component.resume()

    return up_stack_component_command


def generate_stack_component_down_command(
    component_type: StackComponentFlavor,
) -> Callable[[Optional[str], bool], None]:
    """Generates a `down` command for the specific stack component type."""

    @click.argument("name", type=str, required=False)
    @click.option(
        "--force",
        "-f",
        is_flag=True,
        help="Deprovisions local resources instead of suspending them.",
    )
    def down_stack_component_command(
        name: Optional[str] = None, force: bool = False
    ) -> None:
        """Stops/Tears down the local deployment of a stack component."""
        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        component = _get_stack_component(component_type, component_name=name)
        display_name = component_display_name_helper(component_type)

        if not force:
            if not component.is_suspended:
                cli_utils.info(
                    f"Suspending local resources for {display_name} "
                    f"'{component.name}'."
                )
                try:
                    component.suspend()
                except NotImplementedError:
                    cli_utils.error(
                        f"Provisioning local resources not implemented for "
                        f"{display_name} '{component.name}'. If you want to "
                        f"deprovision all resources for this component, use the "
                        f"`--force/-f` flag."
                    )
            else:
                cli_utils.info(
                    f"No running resources found for {display_name} "
                    f"'{component.name}'"
                )
        else:   
            if component.is_provisioned:
                cli_utils.info(
                    f"Deprovisioning resources for {display_name} "
                    f"'{component.name}'."
                )
                component.deprovision()
            else:
                cli_utils.info(
                    f"No provisioned resources found for {display_name} "
                    f"'{component.name}'."
                )

    return down_stack_component_command


def generate_stack_component_logs_command(
    component_type: StackComponentFlavor,
) -> Callable[[Optional[str], bool], None]:
    """Generates a `logs` command for the specific stack component type."""

    @click.argument("name", type=str, required=False)
    @click.option(
        "--follow",
        "-f",
        is_flag=True,
        help="Follow the log file instead of just displaying the current logs.",
    )
    def stack_component_logs_command(
        name: Optional[str] = None, follow: bool = False
    ) -> None:
        """Displays stack component logs."""
        cli_utils.print_active_profile()
        cli_utils.print_active_stack()

        component = _get_stack_component(component_type, component_name=name)
        display_name = component_display_name_helper(component_type)
        log_file = component.log_file

        if not log_file or not fileio.exists(log_file):
            cli_utils.warning(
                f"Unable to find log file for {display_name} "
                f"'{component.name}'."
            )
            return

        if follow:
            try:
                with open(log_file, "r") as f:
                    # seek to the end of the file
                    f.seek(0, 2)

                    while True:
                        line = f.readline()
                        if not line:
                            time.sleep(0.1)
                            continue
                        line = line.rstrip("\n")
                        click.echo(line)
            except KeyboardInterrupt:
                cli_utils.info(f"Stopped following {display_name} logs.")
        else:
            with open(log_file, "r") as f:
                click.echo(f.read())

    return stack_component_logs_command


def generate_stack_component_explain_command(
    component_type: StackComponentFlavor,
) -> Callable[[], None]:
    """Generates an `explain` command for the specific stack component type."""

    def explain_stack_components_command() -> None:
        """Explains the concept of the stack component."""

        component_module = import_module(f"coalescenceml.{component_type.value}")

        if component_module.__doc__ is not None:
            md = Markdown(component_module.__doc__)
            console.print(md)
        else:
            console.print(
                "The explain subcommand is yet not available for "
                "this stack component. For more information, you can "
                "..."
            )

    return explain_stack_components_command


def register_single_stack_component_cli_commands(
    component_type: StackComponentFlavor, parent_group: click.Group
) -> None:
    """Registers all basic stack component CLI commands."""
    command_name = component_type.value.replace("_", "-")
    display_name = component_display_name_helper(component_type)

    @parent_group.group(
        command_name, help=f"Commands to interact with {display_name}."
    )
    def command_group() -> None:
        """Group commands for a single stack component type."""

    # coml stack-component get
    get_command = generate_stack_component_get_command(component_type)
    command_group.command(
        "get", help=f"Get the name of the active {display_name}."
    )(get_command)

    # coml stack-component describe
    describe_command = generate_stack_component_describe_command(component_type)
    command_group.command(
        "describe",
        help=f"Show details about the (active) {display_name}.",
    )(describe_command)

    # coml stack-component list
    list_command = generate_stack_component_list_command(component_type)
    command_group.command(
        "list", help=f"List all registered {display_name}."
    )(list_command)

    # coml stack-component register
    register_command = generate_stack_component_register_command(component_type)
    context_settings = {"ignore_unknown_options": True}
    command_group.command(
        "register",
        context_settings=context_settings,
        help=f"Register a new {display_name}.",
    )(register_command)

    # coml stack-component delete
    delete_command = generate_stack_component_delete_command(component_type)
    command_group.command(
        "delete", help=f"Delete a registered {display_name}."
    )(delete_command)

    # coml stack-component up
    up_command = generate_stack_component_up_command(component_type)
    command_group.command(
        "up",
        help=f"Provisions or resumes local resources for the {display_name} if possible.",
    )(up_command)

    # coml stack-component down
    down_command = generate_stack_component_down_command(component_type)
    command_group.command(
        "down",
        help=f"Suspends resources of the local {display_name} deployment.",
    )(down_command)

    # coml stack-component logs
    logs_command = generate_stack_component_logs_command(component_type)
    command_group.command(
        "logs", help=f"Display {display_name} logs."
    )(logs_command)

    # coml stack-component explain
    explain_command = generate_stack_component_explain_command(component_type)
    command_group.command(
        "explain", help=f"Explaining the {display_name}."
    )(explain_command)


def register_all_stack_component_cli_commands() -> None:
    """Registers CLI commands for all stack components."""
    for component_type in StackComponentFlavor:
        register_single_stack_component_cli_commands(
            component_type, parent_group=cli
        )


register_all_stack_component_cli_commands()
