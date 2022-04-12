from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    TypeVar,
    overload,
)

from coalescenceml.steps import BaseStep
from coalescenceml.steps.utils import (
    INSTANCE_CONFIGURATION,
    OUTPUT_SPEC,
    PARAM_CREATED_BY_FUNCTIONAL_API,
    PARAM_CUSTOM_STEP_OPERATOR,
    PARAM_ENABLE_CACHE,
    STEP_INNER_FUNC_NAME,
)

from coalescenceml.artifacts.base_artifact import BaseArtifact

F = TypeVar("F", bound=Callable[..., Any])

def step(
    func: F,
    *,
    name: Optional[str] = None,
    enable_cache: Optional[bool] = None,
    output_types: Optional[Dict[str, Type[BaseArtifact]]] = None,
    custom_step_operator: Optional[str] = None
) -> Type[BaseStep]:
    """
    """
    step_name = name or func.__name__
    output_spec = output_types or {}

    return type(
        step_name,
        (BaseStep,)
        {
            STEP_INNER_FUNC_NAME: staticmethod(func),
            INSTANCE_CONFIGURATION: {
                PARAM_ENABLE_CACHE: enable_cache,
                PARAM_CREATED_BY_FUNCTIONAL_API: True,
                PARAM_CUSTOM_STEP_OPERATOR: custom_step_operator,
            },
            OUTPUT_SPEC: output_spec,
            "__module__": func.__module__,
        },
    )

@overload
def step(func: F) -> Type[BaseStep]:
    ...
