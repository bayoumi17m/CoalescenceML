import logging


def init_logging() -> None:
    """Initialize logging with default configuration."""


def get_logger(logger_name: str) -> logging.Logger:
    """Fetch logger with name.

    Args:
        logger_name: Name of logger to initialize.

    Returns:
        A logger object.
    """
    return logging.getLogger(logger_name)
