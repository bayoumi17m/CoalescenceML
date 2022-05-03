from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseMLflowDeployer, DeployerConfig
from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from config import DeploymentYAMLConfig
import json
import subprocess
import mlflow
from mlflow.pyfunc.model import (PythonModel)
from typing import Any, Dict

logger = get_logger(__name__)


class LocalDeployer(BaseMLflowDeployer):
    """Step class for deploying model as a local Docker container."""

    def __run_container(self):
        # flags that users may want: port,
        self.__run_cmd(["docker", "run", self.registry_path])

    def entrypoint(self, config : DeployerConfig) -> Dict[str, Any]:
        model_uri, registry_path = self.parse_config(config)
        registry_path = config.registry_path
        self.__build_model_image(model_uri, registry_path)
        self.__push_image(registry_path)
        self.__run_container()
        return ""

    def entrypoint(self, model: PythonModel, config : DeployerConfig) -> Dict[str, Any]:
        self.model_uri = self.get_uri(model)
        model_uri, registry_path = self.parse_config(config)
        registry_path = config.registry_path
        self.__build_model_image(model_uri, registry_path)
        self.__push_image(registry_path)
        self.run_container()
        return ""

deployer = LocalDeployer()

deployment_info = deployer.entrypoint(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model",
    deploy=True
)

print(deployment_info)
