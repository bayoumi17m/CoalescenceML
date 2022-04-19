from typing import Callable, List, Optional, Sequence, Type, TypeVar

from coalescenceml.pipeline.base_pipeline import (
    INSTANCE_CONFIGURATION,
    PARAM_DOCKERIGNORE_FILE,
    PARAM_ENABLE_CACHE,
    PARAM_REQUIRED_INTEGRATIONS,
    PARAM_REQUIREMENTS_FILE,
    PARAM_SECRETS,
    PIPELINE_INNER_FUNC_NAME,
    BasePipeline,
)


F = TypeVar("F", bound=Callable[..., None])


def pipeline(
    func: F,
    *,
    name: Optional[str] = None,
    enable_cache: bool = True,
    required_integrations: Sequence[str] = (),
    requirements_file: Optional[str] = None,
    dockerignore_file: Optional[str] = None,
    secrets: Optional[List[str]] = [],
) -> Type[BasePipeline]:
    """ """
    pipeline_name = name or func.__name__
    return type(
        pipeline_name,
        (BasePipeline,),
        {
            PIPELINE_INNER_FUNC_NAME: staticmethod(func),  # type: ignore[arg-type] # noqa
            INSTANCE_CONFIGURATION: {
                PARAM_ENABLE_CACHE: enable_cache,
                PARAM_REQUIRED_INTEGRATIONS: required_integrations,
                PARAM_REQUIREMENTS_FILE: requirements_file,
                PARAM_DOCKERIGNORE_FILE: dockerignore_file,
                PARAM_SECRETS: secrets,
            },
        },
    )
