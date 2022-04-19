class InitializationException(Exception):
    """Raised when an error occurred during init of a CoML directory."""


class ForbiddenDirectoryAccessError(RuntimeError):
    """Raised when accessing a CoML directory while a step is executed."""
