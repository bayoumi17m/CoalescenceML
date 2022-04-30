import functools
from typing import Any, Callable, Optional, Type, TypeVar, Union, cast, overload

import wandb

from coalescenceml.directory import Directory
from coalescenceml.environment import Environment
from coalescenceml.integrations.wandb.experiment_tracker import (
    WandbExperimentTracker,
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
def enable_wandb(
    step: S,
) -> S:
    """Type annotation for no arguments."""
    ...


@overload
def enable_wandb(
    *, settings: Optional[wandb.Settings] = None,
) -> Callable[[S], S]:
    """Type annotation for arguments."""
    ...


@overload
def enable_wandb(
    step: S,
    *,
    project_name: Optional[str] = None,
    experiment_name: Optional[str] = None,
    settings: Optional[wandb.Settings] = None,
) -> Union[S, Callable[[S], S]]:
    """Decorator to enable wandb run for a step.

    Args:
        step: Step class to be decorated
        project_name: Name of project
        experiment_name: Name of wandb experiment to use. Defaults
            to pipeline name
        settings: Wandb settings to set

    Returns:
        A BaseStep with wrapped entrypoint functionality
    """
    def inner_decorator(step_: S) -> S:
        """Decorate step entrypoint."""

        logger.debug(
            f"Applying wandb decorator to step {step_.__name__}"
        )
        if not issubclass(step_, BaseStep):
            raise RuntimeError(
                "The wandb decorator can only be applied to a BaseStep "
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
                        wandb_entrypoint(
                            project_name=project_name,
                            experiment_name=experiment_name,
                            settings=settings
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


def wandb_entrypoint(
    project_name: Optional[str] = None,
    experiment_name: Optional[str] = None,
    settings: Optional[wandb.Settings] = None,
) -> Callable[[F], F]:
    """Build entrypoint for wandb experiment.

    Args:
        project_name:
        experiment_name:
        settings:

    Returns:
        wrapped function with wandb access
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
            step_env = Environment().step_environment
            run_name = f"{step_env.pipeline_run_id}_{step_env.step_name}"
            tags = (
                step_env.pipeline_name,
                step_env.pipeline_run_id,
            )

            experiment_tracker = Directory(
                skip_repository_check=True,
            ).active_stack.experiment_tracker

            if not isinstance(experiment_tracker, WandbExperimentTracker):
                raise ValueError(
                    "The active stack must have a wandb experiment tracker "
                    "to be registered to be able to track experiments using "
                    "wandb."
                )

            with experiment_tracker.with_wandb_run(
                run_name=run_name,
                tags=tags,
                settings = settings,
            ):
                return func(*args, **kwargs)
        
        return cast(F, wrapper)

    return innter_decorator

