from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseMLflowDeployer
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

    def entrypoint(self, model_uri: str, registry_path: str, deploy: bool) -> Dict[str, Any]:
        if not deploy:
            return None
        self.model_uri = model_uri
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = "mlflow-deployment-service"
        self.__build_model_image()
        self.__push_image()
        self.__run_container()
        return deployment_info

    def entrypoint(self, model: PythonModel, registry_path: str, deploy: bool) -> Dict[str, Any]:
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
