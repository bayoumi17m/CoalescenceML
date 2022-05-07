import json
import os
import subprocess
from coalescenceml.directory import Directory
from coalescenceml.integrations.mlflow.exceptions import ConfigurationError
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import (
    BaseMLflowDeployer,
    BaseDeployerConfig
)
from coalescenceml.integrations.mlflow.step.yaml_config import DeploymentYAMLConfig
from coalescenceml.logger import get_logger

logger = get_logger(__name__)


class KubernetesDeployer(BaseMLflowDeployer):
    """Step class for deploying model to Kubernetes."""

    def config_deployment(self, deployment_name: str, registry_path: str) -> DeploymentYAMLConfig:
        """Configures the deployment.yaml and service.yaml files for deployment."""
        yaml_config = DeploymentYAMLConfig(
            deployment_name, registry_path)
        yaml_config.create_deployment_yaml()
        yaml_config.create_service_yaml()
        return yaml_config

    def deploy(self) -> None:
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.run_cmd(deploy_cmd)
        self.run_cmd(service_cmd)

    def get_deployment_info(self, service_name: str) -> dict:
        """Returns json output of kubectl get service <service_name>."""
        proc = subprocess.run(
            ["kubectl", "get", "service", service_name, "--output=json"],
            capture_output=True,
            check=True
        )
        return json.loads(proc.stdout)

    def entrypoint(self, model_uri: str, config: BaseDeployerConfig) -> dict:
        image_name = config.image_name
        if image_name is None:
            image_name = "mlflow_model_image"
        container_registry = Directory(
            skip_directory_check=True).active_stack.container_registry
        if container_registry is None:
            logger.warning(
                "Container registry not configured. Local image "
                f"{image_name} will be deployed. To configure, use:\n"
                "\"coml container-registry register "
                "<container registry name> --uri=<registry uri> --type="
                "<registry type>\""
            )
            image_path = image_name
            local = True
        else:
            local = False
            registry_path = container_registry.uri
            image_path = os.path.join(registry_path, image_name)
        self.build_model_image(model_uri, image_path)
        # if not local:
        self.push_image(image_path)
        deployment_name = "mlflow-deployment"
        yaml_config = self.config_deployment(deployment_name, image_path)
        self.deploy()
        yaml_config.cleanup()
        service_name = "mlflow-deployment-service"
        deployment_info = self.get_deployment_info(service_name)
        logger.info(deployment_info)
        return deployment_info
