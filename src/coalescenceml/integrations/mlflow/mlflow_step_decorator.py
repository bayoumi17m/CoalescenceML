import functools
from typing import Any, Callable, Optional, Type, TypeVar, Union, cast, overload

from coalescenceml.directory import Directory
from coalescenceml.environment import Environment
from coalescenceml.integrations.mlflow.experiment_tracker import (
    MLFlowExperimentTracker,
)
from coalescenceml.logger import get_logger
from coalescenceml.step import BaseStep
from coalescenceml.step.utils import STEP_INNER_FUNC_NAME

logger = get_logger(__name__)


# Entry point function type
F = TypeVar("F", bound=Callable[..., Any])

# Step type
S = TypeVar("S", bound=Type[BaseStep])


@overload
def enable_mlflow(
    step: S,
) -> S:
    """Type annotation for no arguments."""
    ...


@overload
def enable_mlflow(
    experiment_name: str,
) -> Callable[[S], S]:
    """Type annotation for arguments."""
    ...


def enable_mlflow(
    step: S,
    experiment_name: Optional[str] = None,
) -> Union[S, Callable[[S], S]]:
    """Decorator to enable MLFlow run for a step.

    Args:
        step: Step class to be decorated
        experiment_name: Name of mlflow experiment to use. Defaults
            to pipeline name

    Returns:
        A BaseStep with wrapped entrypoint functionality
    """
    def inner_decorator(step_: S) -> S:
        """Decorate step entrypoint."""

        logger.debug(
            f"Applying MLFlow decorator to step {step_.__name__}"
        )
        if not issubclass(step_, BaseStep):
            raise RuntimeError(
                "The MLFlow decorator can only be applied to a BaseStep "
                "subclass or a function decorated with @step from coml."
            )

        entrypoint_orig = getattr(step_, STEP_INNER_FUNC_NAME)
        return cast(
            S,
            type(
                step_.__name__,
                (step_,),
                {
                    STEP_INNER_FUNC_NAME: staticmethod(
                        mlflow_entrypoint(
                            experiment_name=experiment_name,
                        )(entrypoint_orig)
                    ),
                    "__module__": step_.__module__,
                }
            )
        )

    if step is None:
        return inner_decorator
    else:
        return inner_decorator(step)


def mlflow_entrypoint(
    experiment_name: Optional[str] = None,
) -> Callable[[F], F]:
    """Build entrypoint for wandb experiment.

    Args:
        experiment_name:

    Returns:
        wrapped function with mlflow access
    """
    # TODO: Modify the experiment tracker to consider the
    # project name as well as experiment name being passed
    # in explicitly to the decorator.
    def inner_decorator(func: F) -> F:
        """Wrap function with wandb experiment tracker

        Args:
            func: function to wrap

        Returns:
            Wrapped function
        """
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrap function.

            Args:
                *args: pass through
                **kwargs: pass through

            Returns:
                passthrough
            """
            experiment_tracker = Directory(
                skip_directory_check=True,
            ).active_stack.experiment_tracker

            if not isinstance(experiment_tracker, MLFlowExperimentTracker):
                raise ValueError(
                    "The active stack must have an MLFlow experiment tracker "
                    "to be registered to be able to track experiments using "
                    "MLFlow."
            )

            active_run = experiment_tracker.active_run(experiment_name=experiment_name)
            if not active_run:
                raise RuntimeError(
                    "No active MLFlow run configured."
                )

            with active_run:
                return func(*args, **kwargs)
        
        return cast(F, wrapper)

    return inner_decorator

