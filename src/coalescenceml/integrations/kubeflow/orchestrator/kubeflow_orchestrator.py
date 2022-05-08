from __future__ import annotations
import json
from typing import TYPE_CHECKING, Any, Dict, ClassVar, Optional
import os

import urllib3
import kfp
from kfp_server_api.exceptions import ApiException
from pydantic import root_validator

import coalescenceml
from coalescenceml.artifact_store import LocalArtifactStore
from coalescenceml.directory import Directory
from coalescenceml.enums import MetadataContextFlavor
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.orchestrator import BaseOrchestrator
from coalescenceml.integrations.kubeflow.orchestrator.kubeflow_dag_runner import (
    KubeflowDagRunner,
    KubeflowDagRunnerConfig,
)
from coalescenceml.integrations.kubeflow.orchestrator import k3d_deployment_utils
from coalescenceml.utils.source_utils import get_source_root_path
from coalescenceml.stack.stack_component_class_registry import (
    register_stack_component_class
)
from coalescenceml.stack.stack_validator import StackValidator

if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack


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

    @root_validator
    def set_default_kubernetes_context(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Pydantic root_validator that sets the default `kubernetes_context`
        value to the value that is used to create the locally managed k3d
        cluster, if not explicitly set.
        Args:
            values: Values passed to the object constructor
        Returns:
            Values passed to the Pydantic constructor
        """
        if not values.get("kubernetes_context"):
            # not likely, due to Pydantic validation, but mypy complains
            assert "uuid" in values
            if values["use_k3d"]:
                values["kubernetes_context"] = f"k3d-coml-kfp-{str(values['uuid'])[:10]}"
            else:
                raise ValueError()

        return values

    def image_name_helper(self, image_name):
        return (
            f"{Directory().active_stack.container_registry.uri}/"
            f"{image_name}:latest"
        )

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
        from coalescenceml.utils.docker_utils import build_docker_image

        # Containerize all steps using docker utils helper functions
        image_name = self.image_name_helper(pipeline.name)

        requirements = {*stack.requirements(), *pipeline.requirements}
        
        build_docker_image(
            build_context_path=get_source_root_path(),
            image_name=image_name,
            dockerignore_path=pipeline.dockerignore_file,
            requirements=requirements,
        )

        stack.container_registry.push_image(image_name)

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
        from coalescenceml.environment import Environment

        if Environment.in_notebook():
            raise RuntimeError("Can not run in notebook")

        logger.info("Establishing host connection")

        from coalescenceml.utils.docker_utils import get_image_digest

        image_name = self.image_name_helper(pipeline.name)
        image_name = get_image_digest(image_name) or image_name
        
        fileio.makedirs(self.pipeline_directory)
        pipeline_file_path = os.path.join(
            self.pipeline_directory, f"{pipeline.name}.yaml",
        )

        runner_config = KubeflowDagRunnerConfig(image=image_name)
        runner = KubeflowDagRunner(config=runner_config, output_path=pipeline_file_path)

        logger.info("Proceeding to build pipeline")
        runner.run(
            pipeline=pipeline,
            stack=stack,
            runtime_configuration=runtime_configuration,
        )

        try:

            client = kfp.Client(kube_context=self.kubernetes_context)

            if runtime_configuration.schedule:
                try:
                    experiment = client.get_experiment(pipeline.name)
                except (ValueError, ApiException) as e:
                    experiment = client.create_experiment(pipeline.name)
                
                schedule = runtime_configuration.schedule
                
                result = client.create_recurring_run(
                    experiment_id=experiment.id,
                    job_name=runtime_configuration.run_name,
                    pipeline_package_path=pipeline_file_path,
                    enable_caching=pipeline.enable_cache,
                    start_time=schedule.utc_start_time,
                    end_time=schedule.utc_end_time,
                    interval_second=schedule.interval_second,
                    no_catchup=not schedule.catchup,
                )
            else:
                
                result = client.create_run_from_pipeline_package(
                    pipeline_file_path,
                    arguments={},
                    run_name=runtime_configuration.run_name,
                    enable_caching=pipeline.enable_cache,
                )

                if self.synchronous:
                    client.wait_for_run_completion(
                        run_id = result.run_id, timeout=1200,
                    )

            logger.info("Running pipeline")
        
        except urllib3.exceptions.HTTPError as error:
            logger.warning(
                "Failed to upload."
            )
    
    @property
    def _k3d_cluster_name(self) -> str:
        return f"coml-kfp-{str(self.uuid)[:10]}"
    
    @staticmethod
    def _k3d_registry_name(port: int) -> str:
        return f"k3d-coml-kfp-registry.localhost:{port}"
    
    @property
    def _k3d_registry_config_path(self) -> str:
        """Returns the path to the K3D registry config yaml."""
        return os.path.join(self.root_directory, "k3d_registry.yaml")
    
    @property
    def is_cluster_provisioned(self) -> bool:
        if not self.use_k3d:
            True
        return k3d_deployment_utils.k3d_cluster_exists(
            cluster_name=self._k3d_cluster_name,
        )
    
    @property
    def is_provisioned(self) -> bool:
        if not k3d_deployment_utils.check_prereqs(
            skip_k3d=not self.use_k3d,
        ):
            return False

        return self.is_cluster_provisioned
    
    @property
    def is_running(self) -> bool:
        return (
            self.is_provisioned
            and self.is_cluster_running
            and self.is_daemon_running
        )
    
    @property
    def is_suspended(self) -> bool:
        return (
            self.is_provisioned
            and (not self.is_cluster_running)
            and (not self.is_daemon_running)
        )
    
    @property
    def is_cluster_provisioned(self) -> bool:
        if not self.use_k3d:
            return True
        return k3d_deployment_utils.k3d_cluster_exists(
            cluster_name=self._k3d_cluster_name,
        )
    
    @property
    def is_cluster_running(self) -> bool:
        if not self.use_k3d:
            return True
        return k3d_deployment_utils.k3d_cluster_running(
            cluster_name=self._k3d_cluster_name,
        )
    
    @property
    def pid_file_path(self) -> str:
        return os.path.join(
            self.root_directory,
            "kubeflow_daemon.pid",
        )
    
    @property
    def log_file_path(self) -> str:
        return os.path.join(
            self.root_directory,
            "kubeflow_daemon.log",
        )
    
    @property
    def is_daemon_running(self) -> bool:
        from coalescenceml.utils.daemon import check_if_running
        return check_if_running(self.pid_file_path)
    
    def provision(self) -> None:
        if self.is_running:
            return
        
        if not self.use_k3d:
            return

        if not k3d_deployment_utils.check_prereqs():
            raise RuntimeError()
        
        container_registry = Directory().active_stack.container_registry

        fileio.makedirs(self.root_directory)

        container_registry_port = int(container_registry.uri.split(":")[-1])
        container_registry_name = self._k3d_registry_name(container_registry_port)
        k3d_deployment_utils.write_local_registry_yaml(
            yaml_path=self._k3d_registry_config_path,
            registry_name=container_registry_name,
            registry_uri = container_registry.uri,
        )

        try:
            k3d_deployment_utils.create_k3d_cluster(
                cluster_name=self._k3d_cluster_name,
                registry_name=container_registry_name,
                registry_config_path=self._k3d_registry_config_path,
            )

            kube_context = self.kubernetes_context

            k3d_deployment_utils.deploy_kubeflow_pipelines(
                kubernetes_context=kube_context
            )

            artifact_store = Directory().active_stack.artifact_store
            if isinstance(artifact_store, LocalArtifactStore):
                k3d_deployment_utils.add_hostpath_to_kubeflow_pipelines(
                    kubernetes_context=kube_context,
                    local_path=artifact_store.path,
                )
        except Exception as e:
            logger.error(e)
            self.deprovision()
    
    def deprovision(self) -> None:
        if not self.is_running:
            return
        
        if self.use_k3d:
            # don't deprovision any resources if using a remote KFP installation
            k3d_deployment_utils.delete_k3d_cluster(
                cluster_name=self._k3d_cluster_name
            )
        
        if fileio.exists(self.log_file):
            fileio.remove(self.log_file)
    
    def resume(self) -> None:
        if self.is_running:
            return
        
        if not self.is_provisioned:
            return
        
        kube_context = self.kubernetes_context

        if (
            self.use_k3d
            and not self.is_cluster_running
        ):
            k3d_deployment_utils.start_k3d_cluster(
                cluster_name=self._k3d_cluster_name
            )

            k3d_deployment_utils.wait_until_kubeflow_pipelines_ready(
                kubernetes_context=kube_context
            )
        
        if not self.is_daemon_running:
            k3d_deployment_utils.start_kfp_ui_daemon(
                pid_file_path=self.pid_file_path,
                log_file_path=self.log_file,
                port=self.kfp_ui_port,
                kubernetes_context=kube_context,
            )
    
    def suspend(self) -> None:
        if not self.is_provisioned:
            return
        
        if not self.is_daemon_running:
            k3d_deployment_utils.stop_kfp_ui_daemon(
                pid_file_path=self.pid_file_path
            )
        
        if (
            self.use_k3d
            and self.is_cluster_running
        ):
            # don't suspend any resources if using a remote KFP installation
            k3d_deployment_utils.stop_k3d_cluster(
                cluster_name=self._k3d_cluster_name
            )
