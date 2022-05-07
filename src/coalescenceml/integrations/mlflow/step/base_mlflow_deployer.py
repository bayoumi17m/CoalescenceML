import os
import subprocess
from coalescenceml.logger import get_logger
from coalescenceml.model_deployments.base_deploy_step import BaseDeploymentStep
from coalescenceml.step import BaseStepConfig
from coalescenceml.integrations.exceptions import IntegrationError
from coalescenceml.config.global_config import GlobalConfiguration
import mlflow

logger = get_logger(__name__)


class BaseDeployerConfig(BaseStepConfig):
    image_name: str = None


def get_mlflow_runs_dir() -> str:
    """Returns the path to the mlflow runs directory within the global
    configuration."""
    config_dir = GlobalConfiguration(
    ).config_directory  # Maybe just make mlflow_runs_dir another field of GlobalConfiguration()
    return os.path.join(config_dir, "mlflow_runs")


class BaseMLflowDeployer(BaseDeploymentStep):
    def run_cmd(self, cmd: str) -> None:
        """Helper function for running a bash command."""
        logger.debug(f"Executing command: {' '.join(cmd)}")
        subprocess.run(cmd, text=True, check=True)

    def build_model_image(self, model_uri: str, image_name: str) -> None:
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, as well as the name
        of the image to build."""
        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", model_uri, "-n", image_name]
        self.run_cmd(build_cmd)

    def push_image(self, registry_path: str) -> None:
        """Pushes the docker image to the provided registry path."""
        self.run_cmd(["docker", "push", registry_path])

    def get_uri(self, model):
        """Gets the mlflow uri for the MLFlow model."""
        run_dir = get_mlflow_runs_dir()
        mlflow.set_tracking_uri(run_dir)
        model_info = mlflow.pyfunc.log_model(
            artifact_path="model", python_model=model)
        return model_info.model_uri

    def get_latest_model_uri(self):
        """Returns latest local model run from MLFlow, if any.

        Raises:
            IntegrationError if no such model can be found in the
              config directory.
        """
        runs_dir = get_mlflow_runs_dir()
        if not os.path.exists(runs_dir):
            raise IntegrationError(
                f"MLFlow runs directory not found at {runs_dir}.")
        experiments = [s for s in os.listdir(runs_dir) if s.isnumeric()]
        if len(experiments) == 0:
            raise IntegrationError(
                f"No experiments found in {runs_dir}."
            )
        latest_experiment_dir = os.path.join(runs_dir, max(experiments))
        latest_runs = os.listdir(latest_experiment_dir)
        if len(latest_runs) == 0:
            raise IntegrationError(
                f"No model runs found in {latest_experiment_dir}"
            )
        latest_run = max(latest_runs, key=lambda f: os.path.getctime(
            os.path.join(latest_experiment_dir, f)))
        return os.path.join(latest_experiment_dir, latest_run)
