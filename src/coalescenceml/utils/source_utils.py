import hashlib
import importlib
import inspect
import os
import pathlib
import site
import sys
from contextlib import contextmanager
from types import (
    CodeType,
    FrameType,
    FunctionType,
    MethodType,
    ModuleType,
    TracebackType,
)
from typing import Any, Callable, Iterator, Optional, Type, Union

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
        # The BS that jupyter notebooks make us do...
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


@contextmanager
def prepend_python_path(path: str) -> Iterator[None]:
    """Simple context manager to help import module within the repo"""
    try:
        # Entering the with statement
        sys.path.insert(0, path)
        yield
    finally:
        # Exiting the with statement
        sys.path.remove(path)


def import_class_by_path(class_path: str) -> Type[Any]:
    """Imports a class based on a given path
    
    Args:
        class_path: str, class_source e.g. this.module.Class
    Returns: the given class
    """
    classname = class_path.split(".")[-1]
    modulename = ".".join(class_path.split(".")[0:-1])
    mod = importlib.import_module(modulename)
    return getattr(mod, classname)


def load_source_path_class(
    source: str, import_path: Optional[str] = None
) -> Type[Any]:
    """Loads a Python class from the source.
    Args:
        source: class_source e.g. this.module.Class[@sha]
        import_path: optional path to add to python path
    """
    from coalescenceml.directory import Directory

    repo_root = Directory.find_directory()
    if not import_path and repo_root:
        import_path = str(repo_root)

    if "@" in source:
        source = source.split("@")[0]

    if import_path is not None:
        with prepend_python_path(import_path):
            logger.debug(
                f"Loading class {source} with import path {import_path}"
            )
            return import_class_by_path(source)
    return import_class_by_path(source)


def get_source_root_path() -> str:
    """Get the directory root path or the source root path of the current
    process.

    Returns:
        The source root path of the current process.
    """
    from coalescenceml.directory import Directory

    dir_root = Directory.find_directory()
    if dir_root:
        logger.debug("Using directory root as source root: %s", dir_root)
        return str(dir_root.resolve())

    main_module = sys.modules.get("__main__")
    if main_module is None:
        raise RuntimeError(
            "Could not determine the main module used to run the current "
            "process."
        )

    if not hasattr(main_module, "__file__") or not main_module.__file__:
        raise RuntimeError(
            "Main module was not started from a file. Cannot "
            "determine the module root path."
        )
    path = pathlib.Path(main_module.__file__).resolve().parent

    logger.debug("Using main module location as source root: %s", path)
    return str(path)


def get_module_source_from_module(module: ModuleType) -> str:
    """Gets the source of the supplied module.

    Args:
        module: the module to get the source of.
    Returns:
        The source of the main module.
    Raises:
        RuntimeError: if the module is not loaded from a file
    """
    if not hasattr(module, "__file__") or not module.__file__:
        if module.__name__ == "__main__":
            raise RuntimeError(
                f"{module} module was not loaded from a file. Cannot "
                "determine the module root path."
            )
        return module.__name__
    module_path = os.path.abspath(module.__file__)

    root_path = get_source_root_path()

    if not module_path.startswith(root_path):
        root_path = os.getcwd()
        logger.warning(
            "User module %s is not in the source root. Using current "
            "directory %s instead to resolve module source.",
            module,
            root_path,
        )

    # Remove root_path from module_path to get relative path left over
    module_path = module_path.replace(root_path, "")[1:]

    # Kick out the .py and replace `/` with `.` to get the module source
    module_path = module_path.replace(".py", "")
    module_source = module_path.replace("/", ".")

    logger.debug(
        f"Resolved module source for module {module} to: {module_source}"
    )

    return module_source


def is_standard_source(source: str) -> bool:
    """Returns `True` if source is a standard CoalescenceML source.
    
    Args:
        source: class_source e.g. this.module.Class[@pin].
    """
    if source.split(".")[0] == "coalescenceml":
        return True
    return False


def is_third_party_module(file_path: str) -> bool:
    """Returns whether a file belongs to a third party package."""
    absolute_file_path = pathlib.Path(file_path).resolve()

    for path in site.getsitepackages() + [site.getusersitepackages()]:
        if pathlib.Path(path).resolve() in absolute_file_path.parents:
            return True

    return False


def resolve_class(class_: Type[Any]) -> str:
    """Resolves a class into a serializable source string.
    For classes that are not built-in nor imported from a Python package, the
    `get_source_root_path` function is used to determine the root path
    relative to which the class source is resolved.
    Args:
        class_: A Python Class reference.
    Returns: source_path e.g. this.module.Class.
    """
    initial_source = class_.__module__ + "." + class_.__name__
    if is_standard_source(initial_source):
        return initial_source

    try:
        file_path = inspect.getfile(class_)
    except TypeError:
        # builtin file
        return initial_source

    if initial_source.startswith("__main__") or is_third_party_module(
        file_path
    ):
        return initial_source

    # Regular user file -> get the full module path relative to the
    # source root.
    module_source = get_module_source_from_module(
        sys.modules[class_.__module__]
    )

    # ENG-123 Sanitize for Windows OS
    # module_source = module_source.replace("\\", ".")

    logger.debug(f"Resolved class {class_} to {module_source}")

    return module_source + "." + class_.__name__
