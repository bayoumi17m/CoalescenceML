class StackExistsError(Exception):
    """Raised when trying to register a stack with a name that already
    exists."""


class StackComponentExistsError(Exception):
    """Raised when trying to register a stack component with a name that
    already exists."""

class StackValidationError(Exception):
    """Raised when a stack configuration is not valid."""