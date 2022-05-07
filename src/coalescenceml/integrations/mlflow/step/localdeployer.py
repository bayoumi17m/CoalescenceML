from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import (
    BaseMLflowDeployer, BaseDeployerConfig
)


class LocalDeployer(BaseMLflowDeployer):
    """Step class for deploying model as a local Docker container."""

    def run_container(self, image_name: str, port: int = 5000) -> None:
        """Runs a docker container."""
        self.run_cmd(["docker", "run", "-p", f"{port}:8080", image_name])

    def entrypoint(self, model_uri: str, config: BaseDeployerConfig) -> dict:
        image_name = config.image_name
        if image_name is None:
            image_name = "mlflow_model_image"
        self.build_model_image(model_uri, image_name)
        self.run_container(image_name)
        return {}
