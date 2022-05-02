from typing import Any, ClassVar, Dict

import mlflow
from mlflow.entities import Experiment
from pydantic import validator


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

logger = get_logger(__name__)


@register_stack_component_class
class MLFlowExperimentTracker(BaseExperimentTracker):
    """Stores MLFlow Configuration and interaction functions.

    CoalescenceML configures MLFlow for you....
    """
    # TODO: Ask Iris about local mlflow runs
    tracking_uri: Optional[str] = None # Can this
    is_local: bool
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
