from __future__ import annotations
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Tuple, cast

import tfx.orchestration.pipeline as tfx_pipeline
from tfx.orchestration.portable import data_types, launcher
from tfx.proto.orchestration.pipeline_pb2 import ContextSpec, PipelineNode

from coalescenceml.logger import get_logger
from coalescenceml.directory import Directory
from coalescenceml.step import BaseStep
from coalescenceml.step.utils import (
    INTERNAL_EXECUTION_PARAMETER_PREFIX,
    PARAM_PIPELINE_PARAMETER_NAME,
)
from coalescenceml.utils import readability_utils
    

if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import RuntimeConfiguration
    from coalescenceml.stack import Stack

logger = get_logger(__name__)


def create_tfx_pipeline(
    coalescenceml_pipeline: BasePipeline, stack: Stack
) -> tfx_pipeline.Pipeline:
    """Creates a tfx pipeline from a CoalescenceML pipeline."""
    # Build dag from all steps in the pipeline
    coalescenceml_pipeline.build_dag(**coalescenceml_pipeline.steps)

    tfx_components = [step.component for step in coalescenceml_pipeline.steps.values()]

    artifact_store = stack.artifact_store
    metadata_store = stack.metadata_store

    return tfx_pipeline.Pipeline(
        pipeline_name=coalescenceml_pipeline.name,
        components=tfx_components,
        pipeline_root=artifact_store.path,
        metadata_connection_config=metadata_store.get_tfx_metadata_config(),
        enable_cache=coalescenceml_pipeline.enable_cache,
    )


def get_cache_status(
    execution_info: data_types.ExecutionInfo,
) -> bool:
    """Returns the caching status of a step.

    Args:
        execution_info: The execution info of a `tfx` step.
    Raises:
        AttributeError: If the execution info is `None`.
        KeyError: If no pipeline info is found in the `execution_info`.
    Returns:
        The caching status of a `tfx` step as a boolean value.
    """
    if execution_info is None:
        logger.warning("No execution info found when checking cache status.")
        return False

    status = False
    directory = Directory()
    # TODO: Get the current running stack instead of just the active stack
    # It's possible to deploy a pipeline and switch stacks in which case we'll
    # run into issues....
    # However, the hope is most people will have similar stacks across profiles
    # So it'll inadvertently work LOL.
    active_stack = directory.active_stack
    if not active_stack:
        raise RuntimeError(
            "No active stack is configured for the directory. Run "
            "`coml stack set STACK_NAME` to update the active stack."
        )

    metadata_store = active_stack.metadata_store

    step_name_param = (
        INTERNAL_EXECUTION_PARAMETER_PREFIX + PARAM_PIPELINE_PARAMETER_NAME
    )
    step_name = json.loads(execution_info.exec_properties[step_name_param])
    if execution_info.pipeline_info:
        pipeline_name = execution_info.pipeline_info.id
    else:
        raise KeyError(f"No pipeline info found for step `{step_name}`.")
    pipeline_run_name = cast(str, execution_info.pipeline_run_id)
    pipeline = metadata_store.get_pipeline(pipeline_name)
    if pipeline is None:
        logger.error(f"Pipeline {pipeline_name} not found in Metadata Store.")
    else:
        status = (
            pipeline.get_run(pipeline_run_name).get_step(step_name).is_cached
        )
    return status


def execute_step(
    tfx_launcher: launcher.Launcher,
) -> Optional[data_types.ExecutionInfo]:
    """Executes a tfx component.
    Args:
        tfx_launcher: A tfx launcher to execute the component.
    Returns:
        Optional execution info returned by the launcher.
    """
    step_name_param = (
        INTERNAL_EXECUTION_PARAMETER_PREFIX + PARAM_PIPELINE_PARAMETER_NAME
    )
    pipeline_step_name = tfx_launcher._pipeline_node.node_info.id
    start_time = time.time()
    logger.info(f"Step `{pipeline_step_name}` has started.")
    try:
        execution_info = tfx_launcher.launch()
        if execution_info and get_cache_status(execution_info):
            if execution_info.exec_properties:
                step_name = json.loads(
                    execution_info.exec_properties[step_name_param]
                )
                logger.info(
                    f"Using cached version of `{pipeline_step_name}` "
                    f"[`{step_name}`].",
                )
            else:
                logger.error(
                    f"No execution properties found for step "
                    f"`{pipeline_step_name}`."
                )
    except RuntimeError as e:
        if "execution has already succeeded" in str(e):
            # Hacky workaround to catch the error that a pipeline run with
            # this name already exists. Raise an error with a more descriptive
            # message instead.
            raise RuntimeError("Unable to run a pipeline with a run name that "
                "already exists.")
        else:
            raise e

    run_duration = time.time() - start_time
    logger.info(
        f"Step `{pipeline_step_name}` has finished in "
        f"{readability_utils.get_human_readable_time(run_duration)}."
    )
    return execution_info


def get_step_for_node(node: PipelineNode, steps: List[BaseStep]) -> BaseStep:
    """Finds the matching step for a tfx pipeline node."""
    step_name = node.node_info.id
    try:
        return next(step for step in steps if step.name == step_name)
    except StopIteration:
        raise RuntimeError(f"Unable to find step with name '{step_name}'.")


##### CONTEXT - RELATED UTILITIES #####
def add_context_to_node(
    pipeline_node: PipelineNode,
    type_: str,
    name: str,
    properties: Dict[str, str],
) -> None:
    """
    Add a new context to a TFX protobuf pipeline node.
    Args:
        pipeline_node: A tfx protobuf pipeline node
        type_: The type name for the context to be added
        name: Unique key for the context
        properties: dictionary of strings as properties of the context
    """
    # Add a new context to the pipeline
    context: ContextSpec = pipeline_node.contexts.contexts.add()
    # Adding the type of context
    context.type.name = type_
    # Setting the name of the context
    context.name.field_value.string_value = name
    # Setting the properties of the context depending on attribute type
    for key, value in properties.items():
        c_property = context.properties[key]
        c_property.field_value.string_value = value


def serialize_pydantic_object(
    obj: BaseModel, *, skip_errors: bool = False
) -> Dict[str, str]:
    """Convert a pydantic object to a dict of strings"""

    class PydanticEncoder(json.JSONEncoder):
        def default(self, o: Any) -> Any:
            try:
                return cast(Callable[[Any], str], obj.__json_encoder__)(o)
            except TypeError:
                return super().default(o)

    def _inner_generator(
        dictionary: Dict[str, Any]
    ) -> Iterator[Tuple[str, str]]:
        """Itemwise serialize each element in a dictionary."""
        for key, item in dictionary.items():
            try:
                yield key, json.dumps(item, cls=PydanticEncoder)
            except TypeError as e:
                if skip_errors:
                    logging.info(
                        "Skipping adding field '%s' to metadata context as "
                        "it cannot be serialized due to %s.",
                        key,
                        e,
                    )
                else:
                    raise TypeError(
                        f"Invalid type {type(item)} for key {key} can not be "
                        "serialized."
                    ) from e

    return {key: value for key, value in _inner_generator(obj.dict())}


def add_runtime_configuration_to_node(
    pipeline_node: PipelineNode,
    runtime_config: RuntimeConfiguration,
) -> None:
    """
    Add the runtime configuration of a pipeline run to a protobuf pipeline node.
    Args:
        pipeline_node: a tfx protobuf pipeline node
        runtime_config: a CoalescenceML RuntimeConfiguration
    """
    skip_errors: bool = runtime_config.get(
        "ignore_unserializable_fields", False
    )

    # Determine the name of the context
    def _name(obj: "BaseModel") -> str:
        """Compute a unique context name for a pydantic BaseModel."""
        try:
            return str(hash(obj.json(sort_keys=True)))
        except TypeError as e:
            class_name = obj.__class__.__name__
            logging.info(
                "Cannot convert %s to json, generating uuid instead. Error: %s",
                class_name,
                e,
            )
            return f"{class_name}_{uuid.uuid1()}"

    # iterate over all attributes of runtime context, serializing all pydantic
    # objects to node context.
    for key, obj in runtime_config.items():
        if isinstance(obj, BaseModel):
            logger.debug("Adding %s to context", key)
            add_context_to_node(
                pipeline_node,
                type_=obj.__repr_name__().lower(),
                name=_name(obj),
                properties=serialize_pydantic_object(
                    obj, skip_errors=skip_errors
                ),
            )
