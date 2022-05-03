from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from config import DeploymentYAMLConfig
import json
import subprocess
import mlflow
from typing import Any, Dict

logger = get_logger(__name__)


class LocalDeployer(BaseDeploymentStep):
    """Step class for deploying model as a local Docker container."""

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

    def run_container(self):
        # flags that users may want: port,
        self.run_cmd(["docker", "run", self.registry_path])

    def entrypoint(self, model_uri: str, registry_path: str, deploy: bool) -> Dict[str, Any]:
        if not deploy:
            return None
        self.model_uri = model_uri
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = "mlflow-deployment-service"
        self.build_model_image()
        self.push_image()
        self.run_container()
        return deployment_info

    def entrypoint(self, model, registry_path: str, deploy: bool) -> Dict[str, Any]:
        if not deploy:
            return None
        self.model_uri = self.get_uri(model)
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = "mlflow-deployment-service"
        self.build_model_image()
        self.push_image()
        self.run_container()
        return deployment_info
    
    def get_uri(self, model):
        mlflow.set_tracking_uri()
        model_info = mlflow.pyfunc.log_model(model)
        return model_info.model_uri


kd = KubernetesDeployer()


deployment_info = kd.entrypoint(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model",
    deploy=True
)

print(deployment_info)
