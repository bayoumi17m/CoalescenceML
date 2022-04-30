import os
from contextlib import contextmanager
from typing import ClassVar, Iterator, Optional, Tuple

import wandb

from coalescenceml.integrations.contants import WANDB
from coalescenceml.logger import get_logger
from coalescenceml.stack.stack_component_registry import (
    register_stack_component_class,
)

logger = get_logger(__name__)

WANDB_API_KEY = "WANDB_API_KEY"


@register_stack_component_class
class WandbExperimentTracker(BaseExperimentTracker):
    """Store wandb configuration and execute logging.

    ..note: One method of directly accessing the experiment
        tracker is as follows with the StepContext....

    ```python
    from coalescenceml.step import StepContext


    @enable_wandb
    @step
    def my_train_step(context: StepContext, ...):
        context.stack.experiment_tracker # Get experiment tracker directly
    ```

    ..note: Otherwise directly using wand.log({"acc": 0.5}) will work
        perfectly!

    # TODO: Determine an easy way to access the experiment tracker and
    # the corresponding helper functions without accessing the context.
    # This might be an OK pattern but might be tough to remember
    """

    api_key: str
    entity: Optional[str] = None
    project_name: Optional[str] = None

    FLAVOR: ClassVar[str] = WANDB

    def prepare_step_run(self) -> None:
        """Set the wandb API key."""
        os.environ[WANDB_API_KEY] = self.api_key

    def cleanup_step_run(self) -> None:
        """Clean up wandb API key."""
        os.environ.pop(WANDB_API_KEY, None)

    @contextmanager
    def with_wandb_run(
        self,
        run_name: str,
        tags: Tuple[str, ...] = ()
        settings: Optional[wandb.Settings] = None,
    ) -> Iterator[None]:
        """Activate wandb run for duration of context manager.

        Anything logged to wandb that is run while this is active
        will automatically log to the same wandb run configured
        by the run name passed here.
  
        Args:
            run_name: Name of the wandb run to create
            tags: tags to attach to wandb run
            settings: Additional settings for the wandb run
        """
        try:
            logger.info(
                f"Initializing wandb with project name: {self.project_name} "
                f"run name: {run_name} entity: {self.entity}"
            )
            wandb.init(
                project=self.project_name,
                name=run_name,
                entity=self.entity,
                settings=settings,
                tags=tags,
            )
            yield
        finally:
            wandb.finish()

    def log_params(self, params: Dict[str, Any]) -> None:
        wandb.config.update(params)

    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        wandb.log(metrics)

    def log_artifacts(self, artifacts: Dict[str, Any]) -> None:
        raise NotImplementedError()
