from __future__ import annotations
from importlib.metadata import entry_points

import json
from typing import TYPE_CHECKING, Any, Dict, ClassVar

import kfp
from coalescencml.directory import Directory
from tfx.dsl.compiler.compiler import Compiler
from tfx.dsl.compiler.constants import PIPELINE_RUN_ID_PARAMETER_NAME
from tfx.dsl.components.base import base_component
from tfx.orchestration import metadata
from tfx.orchestration.local import runner_utils
from tfx.orchestration.pipeline import Pipeline as TfxPipeline
from tfx.orchestration.portable import launcher, runtime_parameter_utils
from tfx.proto.orchestration import executable_spec_pb2
from tfx.proto.orchestration.pipeline_pb2 import (
    Pipeline as Pb2Pipeline,
    PipelineNode,
)
from tfx.orchestration.kubeflow import kubeflow_dag_runner

from coalescenceml.directory import Directory
from coalescenceml.enums import MetadataContextFlavor
from coalescenceml.logger import get_logger
from coalescenceml.integrations.kubeflow import orchestrator
from coalescenceml.orchestrator import BaseOrchestrator
from coalescenceml.integrations.kubeflow.orchestrator import utils
from coalescenceml.utils import docker_utils

if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack, StackValidator


logger = get_logger(__name__)


class KubeflowStackValidator(StackValidator):
    """
      Abstract Stack Validator that validates stack compatibility with Kubeflow
    """

    def __init__(self, host: str):
        self.host = host

    def validate(self, stack: Stack) -> bool:
        """
        Validates the stack to ensure all is compatible with KubeFlow
        """
        if stack.artifact_store.FLAVOR == 'local' and self.host == 'local':
            return False
        elif stack.container_registry.is_running and self.host == 'local':
            return False
        return True


class KubeflowOrchestrator(BaseOrchestrator):
    """Orchestrator responsible for running pipelines on Kubeflow."""
    host: ClassVar[str] = 'local'
    output_dir: ClassVar[str] = None
    output_filename: ClassVar[str] = None
    build_context_path: ClassVar[str] = None
    # Class Configuration
    FLAVOR: ClassVar[str] = "kubeflow"
    image_name: ClassVar[str] = None
    entrypoints: ClassVar[list[str]] = []

    def prepare_pipeline_deployment(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> None:
        """Prepares deploying the pipeline.

        Args:
            pipeline: The pipeline to be deployed.
            stack: The stack to be deployed.
            runtime_configuration: The runtime configuration to be used.
        """

        # Containerize all steps using docker utils helper functions
        self.image_name = Directory().active_stack.container_registry.uri + \
            + '/' + pipeline.name + ':latest'
        build_context_path = " ." if self.build_context_path == None else \
            self.build_context_path
        docker_utils.build_docker_image(
            build_context_path=build_context_path, image_name=self.image_name)
        docker_utils.push_docker_image(image_name=self.image_name)

    @property
    def validator(self) -> StackValidator:
        """Returns the validator for this component."""
        return KubeflowStackValidator(self.host)

    @property
    def is_running(self) -> bool:
        """Returns True if the orchestrator is running locally"""
        return True if self.host == 'local' else False

    @property
    def runtime_options(self) -> Dict[str, Any]:
        """Runtime options that are available to configure this component.

        The items of the dictionary should map option names (which can be used
        to configure the option in the `RuntimeConfiguration`) to default
        values for the option (or `None` if there is no default value).
        """
        # TODO
        return {}

    def run_pipeline(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> Any:
        """Runs a pipeline with Kubeflow"""
        # Host initialization, if no host specified then use jupyter notebook

        logger.info("Establishing host connection")

        client = kfp.Client(host=self.host)

        # Create a Kubeflow Pipeline
        if self.output_dir:
            runtime_configuration['output_dir'] = self.output_dir
        if self.output_filename:
            runtime_configuration['output_filename'] = self.output_filename

        logger.info("Proceeding to build pipeline")
        kfp_pipeline = utils.create_kfp_pipeline(
            pipeline, stack=stack, RuntimeConfiguration=runtime_configuration, image_name=self.image_name)

        if runtime_configuration is None:
            runtime_configuration = RuntimeConfiguration()

        if runtime_configuration.schedule:
            logger.warning(
                "Kubeflow Orchestrator currently does not support the"
                "use of schedules. The `schedule` will be ignored ")

        logger.info("Running pipeline")
        client.create_run_from_pipeline_package(pipeline_func=kfp_pipeline)
