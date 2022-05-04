from coalescenceml.logger import get_logger
from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseMLflowDeployer
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


class KubernetesDeployer(BaseMLflowDeployer):
    """Step class for deploying model to Kubernetes."""

    def __config_deployment(self, deployment_name, registry_path):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        yaml_config = DeploymentYAMLConfig(
            deployment_name, registry_path)
        yaml_config.create_deployment_yaml()
        yaml_config.create_service_yaml()
        return yaml_config

    def __deploy(self):
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.__run_cmd(deploy_cmd)
        self.__run_cmd(service_cmd)

    def __get_deployment_info(self, service_name) -> Dict[str, Any]:
        p = subprocess.run(["kubectl", "get", "service",
                           service_name, "--output=json"], capture_output=True)
        return json.dumps(json.loads(p.stdout), indent=4, sort_keys=True)

    def entrypoint(self, config: KubernetesDeployerConfig) -> Dict[str, Any]:
        model_uri, registry_path = self.parse_config(config)
        registry_path = config.registry_path
        deployment_name = "mlflow-deployment"
        service_name = "mlflow-deployment-service"
        self.__build_model_image(model_uri, registry_path)
        self.__push_image(registry_path)
        yaml_config = self.__config_deployment(deployment_name, registry_path)
        self.__deploy()
        yaml_config.cleanup()
        return self.__get_deployment_info(service_name)

kd = KubernetesDeployer()
config = KubernetesDeployerConfig(
    model_uri="s3://coml-mlflow-models/sklearn-regression-model", registry_path="us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model", deploy=True
)
print(kd.entrypoint(config))
