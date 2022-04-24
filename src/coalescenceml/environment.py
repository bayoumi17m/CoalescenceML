from __future__ import annotations

import os
import platform
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type, cast

import distro

from coalescenceml.logger import get_logger
from coalescenceml.utils.singleton import SingletonMetaClass


if TYPE_CHECKING:
    from coalescenceml.step import StepEnvironment

logger = get_logger(__name__)


class Environment(metaclass=SingletonMetaClass):
    """Provides environment information.

    Individual environment components can be registered separately to extend
    the global Environment object with additional information (see
    `BaseEnvironmentComponent`).
    """

    def __init__(self) -> None:
        """Initialize an Environment instance.

        Note: Environment is a singleton class, which means this method will
        only get called once. All following `Environment()` calls will return
        the previously initialized instance.
        """
        self._components: Dict[str, "BaseEnvironmentComponent"] = {}

    @property
    def step_is_running(self) -> bool:
        """Return if a step is currently running.

        Returns:
            True if a step is running else False
        """
        from coalescenceml.step import STEP_ENVIRONMENT_NAME

        # A step is considered to be running if there is an active step
        # environment
        return self.has_component(STEP_ENVIRONMENT_NAME)

    @staticmethod
    def get_system_info() -> Dict[str, Any]:
        """Information about the operating system.

        Returns:
            dictionary with os related information
        """
        system = platform.system()

        if system == "Windows":
            release, version, csd, ptype = platform.win32_ver()

            return {
                "os": "windows",
                "windows_version_release": release,
                "windows_version": version,
                "windows_version_service_pack": csd,
                "windows_version_os_type": ptype,
            }

        if system == "Darwin":
            return {"os": "mac", "mac_version": platform.mac_ver()[0]}

        if system == "Linux":
            return {
                "os": "linux",
                "linux_distro": distro.id(),
                "linux_distro_like": distro.like(),
                "linux_distro_version": distro.version(),
            }

        return {"os": "unknown"}

    @staticmethod
    def python_version() -> str:
        """Return the python version of the running interpreter.

        Returns:
            version of python in use
        """
        return platform.python_version()

    @staticmethod
    def in_docker() -> bool:
        """Determine if process is running in a docker container.

        Returns:
            whether process is in docker
        """
        # TODO: Make this more reliable and add test.
        try:
            with open("/proc/1/cgroup", "rt") as ifh:
                info = ifh.read()
                return "docker" in info or "kubepod" in info
        except (FileNotFoundError, Exception):
            return False

    @staticmethod
    def in_google_colab() -> bool:
        """Determine if process is running in a Google Colab.

        Returns:
            whether process is in google colab
        """
        return "COLAB_GPU" in os.environ

    @staticmethod
    def in_notebook() -> bool:
        """Determine if process is running in a notebook.

        Returns:
            whether process is in a jupyter notebook
        """
        if find_spec("IPython") is not None:
            from IPython import get_ipython  # type: ignore

            if get_ipython().__class__.__name__ in [
                "TerminalInteractiveShell",
                "ZMQInteractiveShell",
            ]:
                return True
        return False

    @staticmethod
    def in_paperspace_gradient() -> bool:
        """Determine if process is running in Paperspace Gradient.

        Returns:
            whether process is in paperspace gradient
        """
        return "PAPERSPACE_NOTEBOOK_REPO_ID" in os.environ

    def register_component(
        self, component: "BaseEnvironmentComponent"
    ) -> "BaseEnvironmentComponent":
        """Register an environment component.

        Args:
            component: a BaseEnvironmentComponent instance.

        Returns:
            The newly registered environment component, or the environment
            component that was already registered under the given name.
        """
        if component.NAME not in self._components:
            self._components[component.NAME] = component
            logger.debug(f"Registered environment component {component.NAME}")
            return component
        else:
            logger.warning(
                f"Ignoring attempt to overwrite an existing Environment "
                f"component registered under the name {component.NAME}."
            )
            return self._components[component.NAME]

    def deregister_component(
        self, component: "BaseEnvironmentComponent"
    ) -> None:
        """Deregisters an environment component.

        Args:
            component: a BaseEnvironmentComponent instance.
        """
        if self._components.get(component.NAME) is component:
            del self._components[component.NAME]
            logger.debug(f"Deregistered environment component"
                         f"{component.NAME}")

        else:
            logger.warning(
                f"Ignoring attempt to deregister an inexistent Environment "
                f"component with the name {component.NAME}."
            )

    def get_component(self, name: str) -> Optional["BaseEnvironmentComponent"]:
        """Get the environment component with a known name.

        Args:
            name: the environment component name.

        Returns:
            The environment component that is registered under the given name,
            or None if no such component is registered.
        """
        return self._components.get(name)

    def get_components(
        self,
    ) -> Dict[str, "BaseEnvironmentComponent"]:
        """Get all registered environment components.

        Returns:
            A mapping of comonent names to components
        """
        return self._components.copy()

    def has_component(self, name: str) -> bool:
        """Check if the environment component with name is available.

        Args:
            name: the environment component name.

        Returns:
            `True` if an environment component with the given name is
            currently registered for the given name, `False` otherwise.

        """
        return name in self._components

    def __getitem__(self, name: str) -> "BaseEnvironmentComponent":
        """Get the environment component with the given name.

        Args:
            name: the environment component name.

        Returns:
            `BaseEnvironmentComponent` instance that was registered for the
            given name.

        Raises:
            KeyError: if no environment component is registered for the given
                name.
        """
        if name in self._components:
            return self._components[name]
        else:
            raise KeyError(
                f"No environment component with name {name} is currently "
                f"registered. This could happen for example if you're trying "
                f"to access an environment component that is only available "
                f"in the context of a step function, or, in the case of "
                f"globally available environment components, if a relevant "
                f"integration has not been activated yet."
            )

    @property
    def step_environment(self) -> "StepEnvironment":
        """Get the current step environment component, if one is available.

        This should only be called in the context of a step function.

        Returns:
            The `StepEnvironment` that describes the current step.
        """
        from coalescenceml.steps import STEP_ENVIRONMENT_NAME, StepEnvironment

        return cast(StepEnvironment, self[STEP_ENVIRONMENT_NAME])


_BASE_ENVIRONMENT_COMPONENT_NAME = "base_environment_component"


class EnvironmentComponentMeta(type):
    """Register EnvironmentComponent instances globally with this Metaclass."""

    def __new__(
        mcs, name: str, bases: Tuple[Type[Any], ...], dct: Dict[str, Any]
    ) -> "EnvironmentComponentMeta":
        """Create BaseEnvironmentComponent class with hook.

        Args:
            name: Class name
            bases: Base classes
            dct: dictionary of attributes

        Returns:
            An environment component casted as BaseEnvironmentComponent
        """
        cls = cast(
            Type["BaseEnvironmentComponent"],
            super().__new__(mcs, name, bases, dct),
        )
        if name != "BaseEnvironmentComponent":
            assert (
                cls.NAME and cls.NAME != _BASE_ENVIRONMENT_COMPONENT_NAME
            ), ("You should specify a unique NAME when creating an"
                "EnvironmentComponent !")
        return cls


class BaseEnvironmentComponent(metaclass=EnvironmentComponentMeta):
    """Base Environment component class.

    All Environment components must inherit from this class and provide a
    unique value for the `NAME` attribute.


    Different code components can independently contribute with information to
    the global Environment by extending and instantiating this class:

    ```python
    from coalescenceml.environment import BaseEnvironmentComponent

    MY_ENV_NAME = "my_env"

    class MyEnvironmentComponent(BaseEnvironmentComponent):

        NAME = MY_ENV_NAME

        def __init__(self, my_env_attr: str) -> None:
            super().__init__()
            self._my_env_attr = my_env_attr

        @property
        def my_env_attr(self) -> str:
            return self._my_env_attr

    my_env = MyEnvironmentComponent()
    ```


    There are two ways to register and deregister a `BaseEnvironmentComponent`
    instance with the global Environment:

    1. by explicitly calling its `activate` and `deactivate` methods:

    ```python
    my_env.activate()

    # ... environment component is active
    # and registered in the global Environment

    my_env.deactivate()

    # ... environment component is not active
    ```

    2. by using the instance as a context:

    ```python
    with my_env:
        # ... environment component is active
        # and registered in the global Environment

    # ... environment component is not active
    ```

    While active, environment components can be discovered and accessed from
    the global environment:

    ```python
    from foo.bar.my_env import MY_ENV_NAME
    from coalescenceml.environment import Environment

    my_env = Environment.get_component(MY_ENV_NAME)

    # this works too, but throws an error if the component is not active:

    my_env = Environment[MY_ENV_NAME]
    ```

    Attributes:
        NAME: a unique name for this component. This name will be used to
            register this component in the global Environment and to
            subsequently retrieve it by calling `Environment().get_component`.
    """

    NAME: str = _BASE_ENVIRONMENT_COMPONENT_NAME

    def __init__(self) -> None:
        """Initialize an environment component."""
        self._active = False

    def activate(self) -> None:
        """Activate environment component and register in global Environment.

        Raises:
            RuntimeError: if the component is already active.
        """
        if self._active:
            raise RuntimeError(
                f"Environment component {self.NAME} is already active."
            )
        Environment().register_component(self)
        self._active = True

    def deactivate(self) -> None:
        """Deactivate component and deregister it from global Environment.

        Raises:
            RuntimeError: if the component is not active.
        """
        if not self._active:
            raise RuntimeError(
                f"Environment component {self.NAME} is not active."
            )
        Environment().deregister_component(self)
        self._active = False

    @property
    def active(self) -> bool:
        """Check if the environment component is currently active.

        Returns:
            if the environment component is activated
        """
        return self._active

    def __enter__(self) -> "BaseEnvironmentComponent":
        """Environment component context entry point.

        Returns:
            The BaseEnvironmentComponent instance.
        """
        self.activate()
        return self

    def __exit__(self, *args: Any) -> None:
        """Environment component context exit point.

        Args:
            args: any arguments that are passed in
        """
        self.deactivate()
