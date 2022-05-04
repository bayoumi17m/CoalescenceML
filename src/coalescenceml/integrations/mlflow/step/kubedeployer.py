from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
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


class KubernetesDeployer(BaseDeploymentStep):
    """Step class for deploying model to Kubernetes."""

    def __run_cmd(self, cmd):
        logger.debug(f"Executing command: {' '.join(cmd)}")
        proc = subprocess.run(cmd, text=True)
        if proc.returncode != 0:
            logger.error(f"Command failed: {proc.stderr}")
            exit(1)

    def __build_model_image(self):
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, and the path to the
        container registry to build the image.
        """
        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", self.model_uri, "-n", self.registry_path]
        self.__run_cmd(build_cmd)

    # Not sure if this step is actually needed
    def __push_image(self):
        """Pushes the docker image to the provided registry path."""
        self.__run_cmd(["docker", "push", self.registry_path])

    def __config_deployment(self):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        self.yaml_config = DeploymentYAMLConfig(
            self.deployment_name, self.registry_path)
        self.yaml_config.create_deployment_yaml()
        self.yaml_config.create_service_yaml()

    def __deploy(self):
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.__run_cmd(deploy_cmd)
        self.__run_cmd(service_cmd)

    def __get_deployment_info(self) -> Dict[str, Any]:
        p = subprocess.run(["kubectl", "get", "service",
                           self.service_name, "--output=json"], capture_output=True)
        return json.dumps(json.loads(p.stdout), indent=4, sort_keys=True)

    def entrypoint(self, config: KubernetesDeployerConfig) -> Dict[str, Any]:
        if not config.deploy:
            return None
        if config.model_uri is not None:
            self.model_uri = config.model_uri
        else:
            config_dir = GlobalConfiguration().config_directory
            runs_dir = os.path.join(config_dir, "mlflow_runs")
            if not os.path.exists(runs_dir):
                raise IntegrationError(
                    f"Error: MLFlow runs directory not found in {config_dir}")
            latest_version_dir = max(
                [s for s in os.listdir(runs_dir) if s.isnumeric()])
            latest_run_path = os.path.join(runs_dir, latest_version_dir)
            latest_run = max(os.listdir(lastest_run_path), key=lambda f: os.path.getctime(
                os.path.join(latest_run_path, f)))
            self.model_uri = os.path.join(latest_run_path, latest_run)
        if config.registry_path is None:
            logger.error("Please specify a registry path for the model image.")
            exit(1)
        self.registry_path = config.registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = "mlflow-deployment-service"
        self.__build_model_image()
        self.__push_image()
        self.__config_deployment()
        self.__deploy()
        self.yaml_config.cleanup()
        return self.__get_deployment_info()
