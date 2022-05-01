from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union, overload

from coalescenceml.artifacts.base_artifact import BaseArtifact
from coalescenceml.step import BaseStep
from coalescenceml.step.utils import (
    INSTANCE_CONFIGURATION,
    OUTPUT_SPEC,
    PARAM_CREATED_BY_FUNCTIONAL_API,
    PARAM_CUSTOM_STEP_OPERATOR,
    PARAM_ENABLE_CACHE,
    STEP_INNER_FUNC_NAME,
)


F = TypeVar("F", bound=Callable[..., Any])


@overload
def step(func: F) -> Type[BaseStep]:
    ...

def step(
    *,
    name: Optional[str] = None,
    enable_cache: Optional[bool] = None,
    output_types: Optional[Dict[str, Type[BaseArtifact]]] = None,
    custom_step_operator: Optional[str] = None,
) -> Callable[[F], Type[BaseStep]]:
    ...

def step(
    func: F = None,
    *,
    name: Optional[str] = None,
    enable_cache: Optional[bool] = None,
    output_types: Optional[Dict[str, Type[BaseArtifact]]] = None,
    custom_step_operator: Optional[str] = None
) -> Union[Callable[[F], Type[BaseStep]], Type[BaseStep]]:
    """ """
    def step_decor(func_: F) -> Type[BaseStep]:
        step_name = name or func_.__name__
        output_spec = output_types or {}

        return type(
            step_name,
            (BaseStep,),
            {
                STEP_INNER_FUNC_NAME: staticmethod(func),
                INSTANCE_CONFIGURATION: {
                    PARAM_ENABLE_CACHE: enable_cache,
                    PARAM_CREATED_BY_FUNCTIONAL_API: True,
                    PARAM_CUSTOM_STEP_OPERATOR: custom_step_operator,
                },
                OUTPUT_SPEC: output_spec,
                "__model__": func.__module__,
            },
        )

    if func is None:
        return step_decor
    else:
        return step_decor(func)
