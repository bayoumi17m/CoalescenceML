from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import (
    BaseMLflowDeployer, BaseDeployerConfig
)
from coalescenceml.logger import get_logger

logger = get_logger(__name__)


class LocalDeployer(BaseMLflowDeployer):
    """Step class for deploying model as a local Docker container."""

    def run_container(self, image_name: str, port: int) -> None:
        """Runs a docker container."""
        self.run_cmd(["docker", "run", "-d", "-p", f"{port}:8080", image_name])

    def entrypoint(self, model_uri: str, config: BaseDeployerConfig, port: int = 5000) -> dict:
        image_name = config.image_name
        if image_name is None:
            image_name = "mlflow_model_image"
        self.build_model_image(model_uri, image_name)
        port = 5000
        self.run_container(image_name, port=port)
        logger.info(f"Model is being served at http://127.0.0.1:{port}/")
        return {}
