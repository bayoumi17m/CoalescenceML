from typing import Optional

import click

import coalescenceml
from coalescenceml.cli import utils as cli_utils
from coalescenceml.cli.cli import cli
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import console
from coalescenceml.directory import Directory
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack import Stack
from coalescenceml.stack.exceptions import ProvisioningError
from coalescenceml.stack.stack_component_class_registry import (
    StackComponentClassRegistry,
)
from coalescenceml.utils import yaml_utils


# Stacks
@cli.group()
def stack() -> None:
    """Stacks to define various environments."""



@stack.command("register", context_settings=dict(ignore_unknown_options=True))
@click.argument("stack_name", type=str, required=True)
@click.option(
    "-m",
    "--metadata-store",
    "metadata_store_name",
    help="Name of the metadata store for this stack.",
    type=str,
    required=True,
)
@click.option(
    "-a",
    "--artifact-store",
    "artifact_store_name",
    help="Name of the artifact store for this stack.",
    type=str,
    required=True,
)
@click.option(
    "-o",
    "--orchestrator",
    "orchestrator_name",
    help="Name of the orchestrator for this stack.",
    type=str,
    required=True,
)
@click.option(
    "-c",
    "--container_registry",
    "container_registry_name",
    help="Name of the container registry for this stack.",
    type=str,
    required=False,
)
@click.option(
    "-x",
    "--secrets_manager",
    "secrets_manager_name",
    help="Name of the secrets manager for this stack.",
    type=str,
    required=False,
)
@click.option(
    "-s",
    "--step_operator",
    "step_operator_name",
    help="Name of the step operator for this stack.",
    type=str,
    required=False,
)
@click.option(
    "-f",
    "--feature_store",
    "feature_store_name",
    help="Name of the feature store for this stack.",
    type=str,
    required=False,
)
@click.option(
    "-d",
    "--model_deployer",
    "model_deployer_name",
    help="Name of the model deployer for this stack.",
    type=str,
    required=False,
)
def register_stack(
    stack_name: str,
    metadata_store_name: str,
    artifact_store_name: str,
    orchestrator_name: str,
    container_registry_name: Optional[str] = None,
    secrets_manager_name: Optional[str] = None,
    step_operator_name: Optional[str] = None,
    feature_store_name: Optional[str] = None,
    model_deployer_name: Optional[str] = None,
) -> None:
    """Register a stack."""
    _register_stack_helper(
        stack_name=stack_name,
        metadata_store_name=metadata_store_name,
        artifact_store_name=artifact_store_name,
        orchestrator_name=orchestrator_name,
        container_registry_name=container_registry_name,
        secrets_manager_name=secrets_manager_name,
        step_operator_name=step_operator_name,
        feature_store_name=feature_store_name,
        model_deployer_name=model_deployer_name,
    )


def _register_stack_helper(
    stack_name: str,
    metadata_store_name: str,
    artifact_store_name: str,
    orchestrator_name: str,
    container_registry_name: Optional[str] = None,
    secrets_manager_name: Optional[str] = None,
    step_operator_name: Optional[str] = None,
    feature_store_name: Optional[str] = None,
    model_deployer_name: Optional[str] = None,
)-> None:
    cli_utils.print_active_profile()

    with console.status(f"Registering stack '{stack_name}'...\n"):
        dir_ = Directory()

        stack_components = {
            StackComponentFlavor.METADATA_STORE: dir_.get_stack_component(
                StackComponentFlavor.METADATA_STORE, name=metadata_store_name
            ),
            StackComponentFlavor.ARTIFACT_STORE: dir_.get_stack_component(
                StackComponentFlavor.ARTIFACT_STORE, name=artifact_store_name
            ),
            StackComponentFlavor.ORCHESTRATOR: dir_.get_stack_component(
                StackComponentFlavor.ORCHESTRATOR, name=orchestrator_name
            ),
        }

        if container_registry_name:
            stack_components[
                StackComponentFlavor.CONTAINER_REGISTRY
            ] = dir_.get_stack_component(
                StackComponentFlavor.CONTAINER_REGISTRY,
                name=container_registry_name,
            )

        if secrets_manager_name:
            stack_components[
                StackComponentFlavor.SECRETS_MANAGER
            ] = dir_.get_stack_component(
                StackComponentFlavor.SECRETS_MANAGER,
                name=secrets_manager_name,
            )

        if step_operator_name:
            stack_components[
                StackComponentFlavor.STEP_OPERATOR
            ] = dir_.get_stack_component(
                StackComponentFlavor.STEP_OPERATOR,
                name=step_operator_name,
            )

        if feature_store_name:
            stack_components[
                StackComponentFlavor.FEATURE_STORE
            ] = dir_.get_stack_component(
                StackComponentFlavor.FEATURE_STORE,
                name=feature_store_name,
            )
        if model_deployer_name:
            stack_components[
                StackComponentFlavor.MODEL_DEPLOYER
            ] = dir_.get_stack_component(
                StackComponentFlavor.MODEL_DEPLOYER,
                name=model_deployer_name,
            )

        stack_ = Stack.from_components(
            name=stack_name, components=stack_components
        )
        dir_.register_stack(stack_)
        cli_utils.info(f"Stack '{stack_name}' successfully registered!")


@stack.command("list")
def list_stacks() -> None:
    """List all available stacks in the active profile."""
    cli_utils.print_active_profile()

    dir_ = Directory()

    if len(dir_.stack_configurations) == 0:
        cli_utils.warning("No stacks registered!")
        return

    active_stack_name = dir_.active_stack_name

    stack_dicts = []
    for stack_name, stack_configuration in dir_.stack_configurations.items():
        is_active = stack_name == active_stack_name
        stack_config = {
            "ACTIVE": ":point_right:" if is_active else "",
            "STACK NAME": stack_name,
            **{
                component_type.value.upper(): value
                for component_type, value in stack_configuration.items()
            },
        }
        stack_dicts.append(stack_config)

    cli_utils.print_table(stack_dicts)


@stack.command(
    "describe",
    help="Show details about the current active stack.",
)
@click.argument(
    "stack_name",
    type=click.STRING,
    required=False,
)
def describe_stack(stack_name: Optional[str]) -> None:
    """Show details about a named stack or the active stack."""
    cli_utils.print_active_profile()

    dir_ = Directory()

    active_stack_name = dir_.active_stack_name
    stack_name = stack_name or active_stack_name

    if not stack_name:
        cli_utils.warning("No stack is set as active!")
        return

    stack_configurations = dir_.stack_configurations
    if len(stack_configurations) == 0:
        cli_utils.warning("No stacks registered!")
        return

    try:
        stack_configuration = stack_configurations[stack_name]
    except KeyError:
        cli_utils.error(f"Stack '{stack_name}' does not exist.")
        return

    cli_utils.print_stack_configuration(
        stack_configuration,
        active=stack_name == active_stack_name,
        stack_name=stack_name,
    )


@stack.command("delete")
@click.argument("stack_name", type=str)
def delete_stack(stack_name: str) -> None:
    """Delete a stack."""
    cli_utils.print_active_profile()

    with console.status(f"Deleting stack '{stack_name}'...\n"):

        cfg = GlobalConfiguration()
        dir_ = Directory()

        if cfg.active_stack_name == stack_name:
            cli_utils.error(
                f"Stack {stack_name} cannot be deleted while it is globally "
                f"active. Please choose a different active global stack first "
                f"by running 'coml stack set --global STACK'."
            )
            return

        if dir_.active_stack_name == stack_name:
            cli_utils.error(
                f"Stack {stack_name} cannot be deleted while it is "
                f"active. Please choose a different active stack first by "
                f"running 'coml stack set STACK'."
            )
            return

        Directory().deregister_stack(stack_name)
        cli_utils.info(f"Deleted stack '{stack_name}'.")


@stack.command("set")
@click.argument("stack_name", type=str)
@click.option(
    "--global",
    "-g",
    "global_profile",
    is_flag=True,
    help="Set the active stack globally.",
)
def set_active_stack(stack_name: str, global_profile: bool = False) -> None:
    """Sets a stack as active.
    If the '--global' flag is set, the global active stack will be set,
    otherwise the directory active stack takes precedence.
    """
    cli_utils.print_active_profile()

    scope = " global" if global_profile else ""

    dir_ = Directory()

    with console.status(
        f"Setting the{scope} active stack to '{stack_name}'..."
    ):

        if global_profile:
            dir_.active_profile.activate_stack(stack_name)
        else:
            dir_.activate_stack(stack_name)

        cli_utils.info(f"Active{scope} stack set to: '{stack_name}'")


@stack.command("get")
def get_active_stack() -> None:
    """Gets the active stack."""
    cli_utils.print_active_profile()

    with console.status("Getting the active stack..."):

        dir_ = Directory()
        cli_utils.info(f"The active stack is: '{dir_.active_stack_name}'")


@stack.command("up")
def up_stack() -> None:
    """Provisions resources for the active stack."""
    cli_utils.print_active_profile()

    stack_ = Directory().active_stack
    cli_utils.info(f"Provisioning resources for active stack '{stack_.name}'.")
    try:
        stack_.provision()
        stack_.resume()
    except ProvisioningError as e:
        cli_utils.error(str(e))


@stack.command("down")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Deprovisions local resources instead of suspending them.",
)
def down_stack(force: bool = False) -> None:
    """Suspends resources of the active stack deployment."""
    cli_utils.print_active_profile()

    stack_ = Directory().active_stack

    if force:
        cli_utils.info(
            f"Deprovisioning resources for active stack '{stack_.name}'."
        )
        stack_.deprovision()
    else:
        cli_utils.info(
            f"Suspending resources for active stack '{stack_.name}'."
        )
        stack_.suspend()


@stack.command("export")
@click.argument("stack_name", type=str, required=True)
@click.option(
    "-f",
    "--filepath",
    "filepath",
    help="filepath to store the stack config",
    type=str,
    required=False,
)
def export_stack(stack_name: str, filepath: str) -> None:
    """Export a stack to a YAML config."""

    # TODO: Dupliate of describe()
    dir_ = Directory()
    stack_configurations = dir_.stack_configurations
    if len(stack_configurations) == 0:
        cli_utils.warning("No stacks registered at all! :((")
        return

    try:
        stack_configuration = stack_configurations[stack_name]
    except KeyError:
        cli_utils.error(f"Stack '{stack_name}' does not exist.")
        return

    # Create dict on stack configuration
    component_data = {}
    for component_type, component_name in stack_configuration.items():
        components = dir_.get_stack_components(component_type)
        for component in components:
            if component.dict()["name"] == component_name:
                component_dict = {
                    key: value
                    for key, value in component.dict().items()
                    if key != "uuid" and value is not None
                }
                print(component_type)
                print(str(component_type))
                component_dict["flavor"] = component.FLAVOR
                component_data[str(component_type)] = component_dict

    # Write out coalescence info as well to YAML
    yaml_data = {
        "coalescenceml_version": coalescenceml.__version__,
        "stack_name": stack_name,
        "components": component_data,
    }

    filepath = filepath or f"{stack_name}-config.yaml"
    yaml_utils.write_yaml(filepath, yaml_data)


@stack.command("import")
@click.argument("filepath", type=str, required=True)
def import_stack(filepath: str) -> None:
    """Import stack from YAML."""
    yaml_data = yaml_utils.read_yaml(filepath)

    # Check COML version
    if yaml_data["coalescenceml_version"] != coalescenceml.__version__:
        cli_utils.error(
            f"Cannot import stacks from other CoalescenceML versions. "
            f"This stack was created using version {yaml_data['coalescenceml_version']} "
            f"while the current version of CoalescenceML is {coalescenceml.__version__}."
        )
        return

    # register components and stack
    stack_name = yaml_data["stack_name"]
    component_data = {}
    for component_type, component_config in yaml_data["components"].items():
        component_data[component_type + "_name"] = component_config["name"]
        # TODO: Duplicate from register component
        component_class = StackComponentClassRegistry.get_class(
            component_type=component_type,
            component_flavor=component_config["flavor"],
        )
        component = component_class(**component_config) # Auto ignores unused kwargs
        Directory().register_stack_component(component)
    _register_stack_helper(stack_name=stack_name, **component_data)
