from coalescenceml.logger import get_logger
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import (
    BaseMLflowDeployer,
    BaseDeployerConfig
)
from coalescenceml.integrations.mlflow.step.yaml_config import DeploymentYAMLConfig
from coalescenceml.step import BaseStepConfig
from coalescenceml.integrations.exceptions import IntegrationError
import json
import os
import subprocess
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.directory import Directory
from sklearn.base import BaseEstimator
from typing import Any, Dict
import mlflow

logger = get_logger(__name__)


class KubernetesDeployer(BaseMLflowDeployer):
    """Step class for deploying model to Kubernetes."""

    def config_deployment(self, deployment_name, registry_path):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        yaml_config = DeploymentYAMLConfig(
            deployment_name, registry_path)
        yaml_config.create_deployment_yaml()
        yaml_config.create_service_yaml()
        return yaml_config

    def deploy(self):
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.run_cmd(deploy_cmd)
        self.run_cmd(service_cmd)

    def get_deployment_info(self, service_name) -> dict:
        proc = subprocess.run(["kubectl", "get", "service",
                               service_name, "--output=json"], capture_output=True)
        return json.loads(proc.stdout)

    def entrypoint(self, model_uri: str, config: BaseDeployerConfig) -> dict:
        container_registry = Directory(skip_directory_check=True).active_stack.container_registry
        if container_registry is None:
            logger.error(
                f"Container registry not configured. Please set the container "
                f"registry uri with coml container-registry register "
                f"<name> --uri=<registry uri> --type="
                f"<registry type>." 
            )
            # Not sure what error to raise here
            exit(1)
        registry_path = container_registry.uri
        deployment_name = "mlflow-deployment"
        service_name = "mlflow-deployment-service"
        image_name = config.image_name
        if image_name is None:
            image_name = "mlflow_model_image"
        image_path = os.path.join(registry_path, image_name)
        self.build_model_image(model_uri, image_path)
        self.push_image(image_path)
        yaml_config = self.config_deployment(deployment_name, image_path)
        self.deploy()
        yaml_config.cleanup()
        deployment_info = self.get_deployment_info(service_name)
        return deployment_info
