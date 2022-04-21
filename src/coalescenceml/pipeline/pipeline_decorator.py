from typing import Callable, List, Optional, Sequence, Type, TypeVar, Union, overload

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


@overload
def pipeline(func: F) -> Type[BasePipeline]:
    ...


@overload
def pipeline(
    *,
    name: Optional[str] = None,
    enable_cache: bool = True,
    required_integrations: Sequence[str] = (),
    requirements_file: Optional[str] = None,
    dockerignore_file: Optional[str] = None,
    secrets: Optional[List[str]] = [],
) -> Type[BasePipeline]:
    ...


def pipeline(
    func: Optional[F] = None,
    *,
    name: Optional[str] = None,
    enable_cache: bool = True,
    required_integrations: Sequence[str] = (),
    requirements_file: Optional[str] = None,
    dockerignore_file: Optional[str] = None,
    secrets: Optional[List[str]] = [],
) -> Union[Callable[[F], Type[BasePipeline]], Type[BasePipeline]]:
    """ """
    def pipe_decorator(func_: F) -> Type[BasePipeline]:
        pipeline_name = name or func_.__name__
        return type(
            pipeline_name,
            (BasePipeline,),
            {
                PIPELINE_INNER_FUNC_NAME: staticmethod(func_),
                INSTANCE_CONFIGURATION: {
                    PARAM_ENABLE_CACHE: enable_cache,
                    PARAM_REQUIRED_INTEGRATIONS: required_integrations,
                    PARAM_REQUIREMENTS_FILE: requirements_file,
                    PARAM_DOCKERIGNORE_FILE: dockerignore_file,
                    PARAM_SECRETS: secrets,
                },
            },
        )

    if func is None:
        return pipe_decorator
    else:
        return pipe_decorator(func)
