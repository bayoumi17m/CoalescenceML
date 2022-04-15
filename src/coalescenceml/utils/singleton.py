"""
https://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons
https://www.geeksforgeeks.org/singleton-pattern-in-python-a-complete-guide/
https://www.geeksforgeeks.org/singleton-method-python-design-patterns/

"""

from typing import Any, Optional, cast


class SingletonMetaClass(type):
    """Singleton metaclass.
    Use this metaclass to make any class into a singleton class:
    ```python
    class BorgSingleton(metaclass=SingletonMetaClass):
        def __init__(self, shared_var):
            self._shared_var = shared_var

        @property
        def shared_var(self):
            return self._shared_var
    first_instance = BorgSingleton('Algorithm')
    second_instance = BorgSingleton('Data Structure')
    print(second_instance.owner)  # Algorithm
    BorgSingleton._clear() # ring destroyed
    ```
    """

    def __init__(cls, *args: Any, **kwargs: Any) -> None:
        """Initialize a singleton class."""
        super().__init__(*args, **kwargs)
        cls.__singleton_instance: Optional["SingletonMetaClass"] = None

    def __call__(cls, *args: Any, **kwargs: Any) -> "SingletonMetaClass":
        """Create or return the singleton instance."""
        if not cls.__singleton_instance:
            cls.__singleton_instance = cast(
                "SingletonMetaClass", super().__call__(*args, **kwargs)
            )

        return cls.__singleton_instance

    def _clear(cls) -> None:
        """Clear the singleton instance."""
        cls.__singleton_instance = None
