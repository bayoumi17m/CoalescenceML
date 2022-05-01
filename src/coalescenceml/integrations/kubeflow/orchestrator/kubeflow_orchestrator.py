from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar

import kfp
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
    def validate(stack: Stack) -> bool:
        """
        Validates the stack to ensure all is compatible with KubeFlow
        """
        if stack.artifact_store.FLAVOR == 'local' and stack.orchestrator.HOST == 'local':
            return False

        return True


class kubeflowOrchestrator(BaseOrchestrator):
    """Orchestrator responsible for running pipelines on Kubeflow."""

    # Class Configuration
    FLAVOR: ClassVar[str] = "kubeflow"
    # HOST: ClassVar[str] = "local"

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
        for step in pipeline.steps:
            # step.containerize()
            # step.componentize() - creates either a string or yaml file that
            # contains the spec for each step for kubeflow
            pass

    @property
    def validator(self) -> StackValidator:
        """Returns the validator for this component."""
        return KubeflowStackValidator()

    def run_pipeline(
        self,
        pipeline: BasePipeline,
        stack: Stack,
        runtime_configuration: RuntimeConfiguration,
    ) -> Any:
        """Runs a pipeline with Kubeflow"""
        # Host initialization, if no host specified then use jupyter notebook
        if runtime_configuration.get("host") is None:
            client = kfp.Client()
        else:
            client = kfp.Client(host=runtime_configuration.get("host"))

        # Create a Kubeflow Pipeline

        kfp_pipeline = utils.create_kfp_pipeline(
            pipeline, stack=stack, RuntimeConfiguration=runtime_configuration
        )

        if runtime_configuration is None:
            runtime_configuration = RuntimeConfiguration()

        if runtime_configuration.schedule:
            logger.warning(
                "Kubeflow Orchestrator currently does not support the"
                "use of schedules. The `schedule` will be ignored ")
        client.create_run_from_pipeline_func(
            pipeline_func=kfp_pipeline, arguments={})
