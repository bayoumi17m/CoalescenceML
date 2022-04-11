import hashlib
import inspect
import sys
from types import (
    CodeType,
    FrameType,
    FunctionType,
    MethodType,
    ModuleType,
    TracebackType,
)
from typing import Any, Callable, Type, Union

from coalescenceml.environment import Environment
from coalescenceml.logger import get_logger

logger = get_logger(__name__)


def get_source(value: Any) -> str:
    """Returns the source code of an object. If executing within a IPython
    kernel environment, then this monkey-patches `inspect` module temporarily
    with a workaround to get source from the cell.
    Raises:
        TypeError: If source not found.
    """
    if Environment.in_notebook():
        # Hacky patch inspect.getfile temporarily to make get_source work.
        # Source: https://stackoverflow.com/questions/51566497/
        def _new_getfile(
            object: Any,
            _old_getfile: Callable[
                [
                    Union[
                        ModuleType,
                        Type[Any],
                        MethodType,
                        FunctionType,
                        TracebackType,
                        FrameType,
                        CodeType,
                        Callable[..., Any],
                    ]
                ],
                str,
            ] = inspect.getfile,
        ) -> Any:
            if not inspect.isclass(object):
                return _old_getfile(object)

            # Lookup by parent module (as in current inspect)
            if hasattr(object, "__module__"):
                object_ = sys.modules.get(object.__module__)
                if hasattr(object_, "__file__"):
                    return object_.__file__ 

            # If parent module is __main__, lookup by methods
            for name, member in inspect.getmembers(object):
                if (
                    inspect.isfunction(member)
                    and object.__qualname__ + "." + member.__name__
                    == member.__qualname__
                ):
                    return inspect.getfile(member)
            else:
                raise TypeError(f"Source for {object!r} not found.")

        # Hacky patch, compute source, then revert Hacky patch.
        _old_getfile = inspect.getfile
        inspect.getfile = _new_getfile
        try:
            src = inspect.getsource(value)
        finally:
            inspect.getfile = _old_getfile
    else:
        # Use standard inspect if running outside a notebook
        src = inspect.getsource(value)
    return src

def get_hashed_source(value: Any) -> str:
    """Returns a hash of the objects source code."""
    try:
        source_code = get_source(value)
    except TypeError:
        raise TypeError(
            f"Unable to compute the hash of source code of object: {value}."
        )
    return hashlib.sha256(source_code.encode("utf-8")).hexdigest()