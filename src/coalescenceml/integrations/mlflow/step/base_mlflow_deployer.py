from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from coalescenceml.integrations.mlflow.step.yaml_config import DeploymentYAMLConfig
from coalescenceml.step import BaseStepConfig
from coalescenceml.integrations.exceptions import IntegrationError 
import subprocess
import os
from coalescenceml.config.global_config import GlobalConfiguration
from typing import Any, Dict
import mlflow
from mlflow.pyfunc.model import (PythonModel)

logger = get_logger(__name__)

class DeployerConfig(BaseStepConfig):
        # model : PythonModel
        model_uri : str = None
        registry_path : str = None
        deploy : bool = True

class BaseMLflowDeployer(BaseDeploymentStep):
    def run_cmd(self, cmd):
        logger.debug(f"Executing command: {' '.join(cmd)}")
        proc = subprocess.run(cmd, text=True)
        if proc.returncode != 0:
            logger.error(f"Command failed: {proc.stderr}")
            exit(1)

    def build_model_image(self, model_uri, registry_path):
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, and the path to the
        container registry to build the image.
        """

        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", model_uri, "-n", registry_path]
        self.run_cmd(build_cmd)

    # Not sure if this step is actually needed
    def push_image(self, registry_path):
        """Pushes the docker image to the provided registry path."""
        self.run_cmd(["docker", "push", registry_path])

    def get_uri(self, model):
        """Gets the mlflow uri for the mlflow model."""
        run_dir = os.path.join(GlobalConfiguration().config_directory, "mlflow_runs")
        mlflow.set_tracking_uri(run_dir)
        model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=model)
        return model_info.model_uri

    def parse_config(self, config : DeployerConfig):
        if not config.deploy:
            return None
        if config.model_uri is not None:
            model_uri = config.model_uri
        else:
            config_dir = GlobalConfiguration().config_directory
            runs_dir = os.path.join(config_dir, "mlflow_runs")
            if not os.path.exists(runs_dir):
                raise IntegrationError(f"Error: MLFlow runs directory not found in {config_dir}")
            latest_version_dir = max([s for s in os.listdir(runs_dir) if s.isnumeric()])
            latest_run_path = os.path.join(runs_dir, latest_version_dir)
            latest_run = max(os.listdir(latest_run_path), key=lambda f : os.path.getctime(os.path.join(latest_run_path, f)))
            model_uri = os.path.join(latest_run_path, latest_run)
            
        if config.registry_path is None:
            logger.error("Please specify a registry path for the model image.")
            exit(1)

        registry_path = config.registry_path

        return model_uri, registry_path
