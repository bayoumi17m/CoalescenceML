from typing import AbstractSet, Callable, Optional

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack.exceptions import StackValidationError
from coalescenceml.logger import get_logger

from coalescenceml.stack import Stack

logger = get_logger(__name__)


class StackValidator:
    """A `StackValidator` is used to validate a stack configuration.

    Each `StackComponent` can provide a `StackValidator` to make sure it is
    compatible with all components of the stack. The `KubeflowOrchestrator`
    for example will always require the stack to have a container registry
    in order to push the docker images that are required to run a pipeline
    in Kubeflow Pipelines.
    """

    def __init__(
        self,
        required_components: Optional[AbstractSet[StackComponentFlavor]] = None,
        custom_validation_function: Optional[Callable[[Stack], bool]] = None,
    ):
        """Initializes a `StackValidator` instance.

        Args:
            required_components: Optional set of stack components that must
                exist in the stack.
            custom_validation_function: Optional function that returns whether
                a stack is valid.
        """
        self._required_components = required_components or set()
        self._custom_validation_function = custom_validation_function

    def validate(self, stack: Stack) -> None:
        """Validates the given stack.

        Checks if the stack contains all the required components and passes
        the custom validation function of the validator.

        Raises:
            StackValidationError: If the stack does not meet all the
                validation criteria.
        """
        missing_components = self._required_components - set(stack.components)
        if missing_components:
            raise StackValidationError(
                f"Missing stack components {missing_components} for "
                f"stack: {stack}"
            )

        if (
            self._custom_validation_function
            and not self._custom_validation_function(stack)
        ):
            raise StackValidationError(
                f"Custom validation function failed to validate "
                f"stack: {stack}"
            )