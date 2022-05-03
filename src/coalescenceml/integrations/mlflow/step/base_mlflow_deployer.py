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
    def __init__(self, model: PythonModel = None, model_uri: str = None, registry_path: str = None, deploy: bool = True):
        self.model: PythonModel = model
        self.model_uri: str = model_uri
        self.registry_path: str = registry_path
        self.deploy: bool = deploy

class BaseMLflowDeployer(BaseDeploymentStep):
    def __init__(self, config: DeployerConfig):
        self.config = config
        
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
        config = self.config
        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", config.model_uri, "-n", config.registry_path]
        self.__run_cmd(build_cmd)

    # Not sure if this step is actually needed
    def __push_image(self):
        """Pushes the docker image to the provided registry path."""
        config = self.config
        self.__run_cmd(["docker", "push", config.registry_path])

    def __get_uri(self, model):
        run_dir = os.path.join(GlobalConfiguration().config_directory, "mlflow_runs")
        mlflow.set_tracking_uri(run_dir)
        model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=model)
        return model_info.model_uri
