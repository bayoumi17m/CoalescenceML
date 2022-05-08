from __future__ import annotations
import json
from typing import TYPE_CHECKING, Any, Dict, ClassVar, Optional
import os

import kfp

import coalescenceml
from coalescenceml.directory import Directory
from coalescenceml.enums import MetadataContextFlavor
from coalescenceml.logger import get_logger
from coalescenceml.orchestrator import BaseOrchestrator
from coalescenceml.integrations.kubeflow.orchestrator import utils
from coalescenceml.utils import docker_utils
from coalescenceml.stack.stack_component_class_registry import (
    register_stack_component_class
)

if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack
from coalescenceml.stack.stack_validator import StackValidator


logger = get_logger(__name__)

DEFAULT_KFP_UI_PORT = 8080


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


@register_stack_component_class
class KubeflowOrchestrator(BaseOrchestrator):
    """Orchestrator responsible for running pipelines on Kubeflow."""
    kubernetes_context: Optional[str] = None
    kfp_ui_port: int = DEFAULT_KFP_UI_PORT
    synchronous: bool = False
    use_k3d: bool = False

    # Class Configuration
    FLAVOR: ClassVar[str] = "kubeflow"

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
        # from coalescenceml.utils.docker_utils import 

        # Containerize all steps using docker utils helper functions
        self.image_name = Directory().active_stack.container_registry.uri + \
            + '/' + pipeline.name + ':latest'
        build_context_path = " ." if self.build_context_path == None else \
            self.build_context_path
        docker_utils.build_docker_image(
            build_context_path=build_context_path, image_name=self.image_name)
        docker_utils.push_docker_image(image_name=self.image_name)

    @property
    def validator(self) -> Optional[StackValidator]:
        """Returns the validator for this component."""
        return None

    @property
    def is_running(self) -> bool:
        """Returns True if the orchestrator is running locally"""
        return True

    @property
    def runtime_options(self) -> Dict[str, Any]:
        """Runtime options that are available to configure this component.

        The items of the dictionary should map option names (which can be used
        to configure the option in the `RuntimeConfiguration`) to default
        values for the option (or `None` if there is no default value).
        """
        # TODO
        return {}
    
    @property
    def root_directory(self) -> str:
        return os.path.join(
            coalescenceml.io.utils.get_global_config_directory(),
            "kubeflow",
            str(self.uuid),
        )
    
    @property
    def pipeline_directory(self) -> str:
        return os.path.join(
            self.root_directory,
            "pipelines",
        )

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
