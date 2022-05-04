import os
from typing import Any, ClassVar, Dict, Optional

import mlflow
from mlflow.entities import Experiment
from pydantic import root_validator, validator


from coalescenceml.directory import Directory
from coalescenceml.environment import Environment
from coalescenceml.experiment_tracker import (
    BaseExperimentTracker,
)
from coalescenceml.integrations.constants import MLFLOW
from coalescenceml.logger import get_logger
from coalescenceml.stack.stack_component_class_registry import (
    register_stack_component_class,
)
from coalescenceml.stack.stack_validator import StackValidator

logger = get_logger(__name__)

MLFLOW_TRACKING_TOKEN = "MLFLOW_TRACKING_TOKEN"
MLFLOW_TRACKING_USERNAME = "MLFLOW_TRACKING_USERNAME"
MLFLOW_TRACKING_PASSWORD = "MLFLOW_TRACKING_PASSWORD"
# TODO: Determine whether to use insecure TLS
# TODO: How do we use a cert bundle if desired?
# https://www.mlflow.org/docs/latest/tracking.html#logging-to-a-tracking-server


@register_stack_component_class
class MLFlowExperimentTracker(BaseExperimentTracker):
    """Stores MLFlow Configuration and interaction functions.

    CoalescenceML configures MLFlow for you....
    """
    # TODO: Ask Iris about local mlflow runs
    tracking_uri: Optional[str] = None # Can this
    use_local_backend: bool = False # base it on tracking_uri
    tracking_token: Optional[str] = None # The way I prefer using MLFlow
    tracking_username: Optional[str] = None
    tracking_password: Optional[str] = None

    FLAVOR: ClassVar[str] = MLFLOW

    @validator("tracking_uri")
    def ensure_valid_tracking_uri(
        cls, tracking_uri: Optional[str] = None,
    ) -> Optional[str]:
        """Ensure the tracking uri is a valid mlflow tracking uri.

        Args:
            tracking_uri: The value to verify

        Returns:
            Valid tracking uri

        Raises:
            ValueError: if mlflow tracking uri is invalid.
        """
        if tracking_uri:
            valid_protocols = DATABASE_ENGINES + ["http", "https", "file"]
            if not any(
                tracking_uri.startswith(protocol)
                for protocol in valid_protocols
            ):
                raise ValueError(
                    f"MLFlow tracking uri does not use a valid protocol "
                    f" which inclue: {valid_protocols}. Please see "
                    f"https://www.mlflow.org/docs/latest/tracking.html"
                    f"#where-runs-are-recorded for more information."
                )

        return tracking_uri

    @staticmethod
    def is_remote_tracking_uri(tracking_uri: str) -> bool:
        """Check whether URI is using remote protocol.

        Args:
            tracking_uri: MLFlow tracking server location

        Returns:
            True if server is remote else False
        """
        return any(
            tracking_uri.startswith(prefix)
            for prefix in ["http", "https"]
            # Only need these as tested with AWS MySQL generates hostname
            # with HTTP. Hence any cloud servers will have to have http and 
            # local ones do not. But this might be wrong and need to change.
        )

    @root_validator
    def ensure_tracking_uri_or_local(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure that the tracking uri exists or use local backend is true.

        Args:
            values: Pydantic stored values of class

        Returns:
            Pydantic class values
        """
        tracking_uri = values.get("tracking_uri")
        use_local_backend = values.get("use_local_backend")

        if not (tracking_uri or use_local_backend):
            raise ValueError(
                f"You must specify either a tracking uri or the use "
                f"your local artifact store for the MLFlow "
                f"experiment tracker."
                # TODO: Message on how to fix.
            )

        return values


    @root_validator
    def ensure_authentication(
        cls, values: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Ensure that credentials exists for a remote tracking instance.

        Args:
            values: Pydantic stored values of class

        Returns:
            class pydantic values
        """
        tracking_uri = values.get("tracking_uri")

        if tracking_uri: # Check if exists
            if cls.is_remote_tracking_uri(tracking_uri): # Check if remote
                token_auth = values.get("tracking_token")

                basic_http_auth = (
                    values.get("tracking_username")
                    and values.get("tracking_password")
                )

                if not (token_auth or basic_http_auth):
                    raise ValueError(
                        f"MLFlow experiment tracking with a remote tracking "
                        f"uri '{tracking_uri}' is only allowed when either a "
                        f"username and password or auth token is used."
                        # TODO: Add update commands to stack to all users to update the component.
                    )

        return values

    @staticmethod
    def local_mlflow_tracker() -> str:
        """Return local mlflow folder inside artifact store.

        Returns:
            MLFlow tracking uri for local backend.
        """
        dir_ = Directory(skip_directory_check=True)
        artifact_store = dir_.active_stack.artifact_store
        # TODO: MLFlow can connect to non-local stores however
        # I am unsure what this entails and how to test this.
        # AWS would be an interesting testbed for a future iteration.
        # Ideally this would work for an arbitrary artifact store.

        local_mlflow_uri = os.path.join(artifact_store.path, "mlruns")
        if not os.path.exists(local_mlflow_uri):
            os.makedirs(local_mlflow_uri)

        return "file:" + local_mlflow_uri

    def get_tracking_uri(self) -> str:
        """Return configured MLFlow tracking uri.

        Returns:
            MLFlow tracking uri
        
        Raises:
            ValueError: if tracking uri is empty and user didn't specify to use the local backend.
        """
        if self.tracking_uri:
            return self.tracking_uri
        else:
            if self.use_local_backend:
                return self.local_mlflow_tracker()
            else:
                raise ValueError(
                    f"You must specify either a tracking uri or the use "
                    f"of your local artifact store for the MLFlow "
                    f"experiment tracker."
                )

    def prepare_step_run(self) -> None:
        """Configure MLFlow tracking URI and credentials."""
        if self.tracking_token:
            os.environ[MLFLOW_TRACKING_TOKEN] = self.tracking_token
        if self.tracking_username:
            os.environ[MLFLOW_TRACKING_USERNAME] = self.tracking_username
        if self.tracking_password:
            os.environ[MLFLOW_TRACKING_PASSWORD] = self.tracking_password

        mlflow.set_tracking_uri(self.get_tracking_uri())
        return

    def cleanup_step_run(self) -> None:
        mlflow.set_tracking_uri("")

    @property
    def validator(self) -> Optional[StackValidator]:
        """Validate that MLFlow config with the rest of stack is valid.

        ..note: We just need to check (for now) that if the use local flag
            is used that there is a local artifact store
        """
        if self.tracking_uri:
            # Tracking URI exists so do nothing
            return None
        else:
            from coalescenceml.artifact_store import LocalArtifactStore
            # Presumably they've set the use_local_backend to true
            # So check for local artifact store b/c thats all that
            # works for now. This will be edited later (probably...)
            return StackValidator(
                custom_validation_function=lambda stack: (
                    isinstance(stack.artifact_store, LocalArtifactStore),
                    "MLFlow experiment tracker with local backend only "
                    "works with a local artifact store at this time."
                )
            )


    def active_experiment(self, experiment_name=None) -> Optional[Experiment]:
        """Return currently active MLFlow experiment.

        Args:
            experiment_name: experiment name to set in MLFlow.

        Returns:
            None if not in step else will return an MLFlow Experiment
        """
        step_env = Environment().step_env

        if not step_env:
            # I.e. we are not in a step running
            return None

        experiment_name = experiment_name or step_env.pipeline_name
        mlflow.set_experiment(experiment_name=experiment_name)
        return mlflow.get_experiment_by_name(experiment_name)

    def active_run(self, experiment_name=None) -> Optional[mlflow.ActiveRun]:
        """"""
        step_env = Environment().step_env
        active_experiment = self.active_experiment(experiment_name)

        if not active_experiment:
            # This checks for experiment + not being in setp
            # Should we make this explicit or keep it as implicit?
            return None

        experiment_id = active_experiment.experiment_id
        # TODO: There may be race conditions in the below code for parallel
        # steps. For example for HP tuning if two train steps are running
        # and they both create a run then we send it onwards to a testing step
        # which will now not know which run to use. We don't want a new run
        # But rather to search for the most recent run. But even that might
        # Have conflicts when there are multiple running steps that do this...
        # How can we handle this?
        # Naive Idea: How about each step has an identifier with it?
        runs = mlflow.search_runs(
            experiment_ids=[experiment_id],
            filter_string=f'tags.mlflow.runName = "{step_env.pipeline_run_id}"',
            output_format="list",
        )

        run_id = runs[0].info.run_id if runs else None

        current_active_run = mlflow.active_run()
        if current_active_run and current_active_run.info.run_id == run_id:
            # Is not None AND run_id matches
            return current_active_run

        return mlflow.start_run(
            run_id = run_id,
            run_name=step_env.pipeline_run_id,
            experiment_id=experiment_id,
        )

    def log_params(self, params: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def log_artifacts(self, artifacts: Dict[str, Any]) -> None:
        raise NotImplementedError()
