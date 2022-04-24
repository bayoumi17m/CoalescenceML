from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from config import DeploymentYAMLConfig
import json
import subprocess
from typing import Any, Dict

logger = get_logger(__name__)


class KubernetesDeployer(BaseDeploymentStep):
    """Step class for deploying model to Kubernetes."""

    def run_cmd(self, cmd):
        logger.debug(f"Executing command: {' '.join(cmd)}")
        proc = subprocess.run(cmd, text=True)
        if proc.returncode != 0:
            logger.error(f"Command failed: {proc.stderr}")
            exit(1)

    # Change to custom Flask app instead of mlflow build-docker
    def build_model_image(self):
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, and the path to the
        container registry to build the image.
        """
        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", self.model_uri, "-n", self.registry_path]
        self.run_cmd(build_cmd)

    # Not sure if this step is actually needed
    def push_image(self):
        """Pushes the docker image to the provided registry path."""
        self.run_cmd(["docker", "push", self.registry_path])

    def config_deployment(self):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        config = DeploymentYAMLConfig(self.deployment_name, self.registry_path)
        config.create_deployment_yaml()
        config.create_service_yaml()

    def deploy(self):
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.run_cmd(deploy_cmd)
        self.run_cmd(service_cmd)

    def get_deployment_info(self) -> Dict[str, Any]:
        p = subprocess.run(["kubectl", "get", "service",
                           self.service_name, "--output=json"], capture_output=True)
        return json.loads(p.stdout)

    def entrypoint(self, model_uri: str, registry_path: str, deploy: bool) -> Dict[str, Any]:
        if not deploy:
            return None
        self.model_uri = model_uri
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = "mlflow-deployment-service"
        self.build_model_image()
        self.push_image()
        self.config_deployment()
        self.deploy()
        deployment_info = self.get_deployment_info()
        return deployment_info


kd = KubernetesDeployer()


deployment_info = kd.entrypoint(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model",
    deploy=True
)

print(deployment_info)
