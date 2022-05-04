from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseMLflowDeployer, DeployerConfig
from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from coalescenceml.integrations.mlflow.step.yaml_config import DeploymentYAMLConfig
import json
import os
import subprocess
import mlflow
from mlflow.pyfunc.model import (PythonModel)
from typing import Any, Dict

logger = get_logger(__name__)


class LocalDeployer(BaseMLflowDeployer):
    """Step class for deploying model as a local Docker container."""

    def run_container(self, registry_path):
        # flags that users may want: port,
        self.run_cmd(["docker", "run", registry_path])

    def serve_model(self, model_uri):
        model_path = os.path.join(model_uri, "artifacts", "model")
        logger.info(model_path)
        self.run_cmd(["mlflow", "models", "serve", "-m", model_path, "-p", "1234"])
    # def entrypoint(self, model: PythonModel, config: DeployerConfig) -> dict:
    #     self.model_uri = self.get_uri(model)
    #     model_uri, registry_path = self.parse_config(config)
    #     registry_path = config.registry_path
    #     self.build_model_image(model_uri, registry_path)
    #     self.push_image(registry_path)
    #     self.run_container()
    #     return {}

    def entrypoint(self, config: DeployerConfig) -> dict:
        model_uri, registry_path = self.parse_config(config)
        # self.build_model_image(model_uri, registry_path)
        # self.run_container(registry_path)
        self.serve_model(model_uri)
        return {}