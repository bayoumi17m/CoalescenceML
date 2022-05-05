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
        self.run_cmd(["docker", "run", "-p", "5000:8080", registry_path])

    def entrypoint(self, config: DeployerConfig) -> dict:
        model_uri, registry_path = self.parse_config(config)
        self.build_model_image(model_uri, registry_path)
        self.run_container(registry_path)
        return {}
