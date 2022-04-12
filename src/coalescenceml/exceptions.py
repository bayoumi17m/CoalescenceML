class InitializationException(Exception):
    """Raised when an error occurred during initialization of a CoML
    directory."""

class ForbiddenDirectoryAccessError(RuntimeError):
    """Raised when trying to access a CoML directory instance while a step
    is executed."""
