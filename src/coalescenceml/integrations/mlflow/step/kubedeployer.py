from coalescenceml.logger import get_logger
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseMLflowDeployer
from coalescenceml.integrations.mlflow.step.yaml_config import DeploymentYAMLConfig
from coalescenceml.step import BaseStepConfig
from coalescenceml.integrations.exceptions import IntegrationError
import json
import os
import subprocess
from coalescenceml.config.global_config import GlobalConfiguration
from sklearn.base import BaseEstimator
from typing import Any, Dict

logger = get_logger(__name__)


class KubernetesDeployerConfig(BaseStepConfig):
    model_uri: str = None
    registry_path: str = None
    deploy: bool = True


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

    def entrypoint(self, config: KubernetesDeployerConfig) -> dict:
        model_uri, registry_path = self.parse_config(config)
        registry_path = config.registry_path
        deployment_name = "mlflow-deployment"
        service_name = "mlflow-deployment-service"
        self.build_model_image(model_uri, registry_path)
        self.push_image(registry_path)
        yaml_config = self.config_deployment(deployment_name, registry_path)
        self.deploy()
        yaml_config.cleanup()
        deployment_info = self.get_deployment_info(service_name)
        logger.info(deployment_info)
        return deployment_info
