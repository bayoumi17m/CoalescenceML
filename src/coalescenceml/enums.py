import logging
from enum import Enum


class LoggingLevels(Enum):
    
    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARN
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

class ExecutionStatus(Enum):

    FAILED = "failed"
    RUNNING = "running"
    CACHED = "cached"
    COMPLETED = "completed"

class StackComponentFormat(Enum):
    """"""

class ArtifactStoreFormat(StackComponentFormat):
    """"""

    AZURE = "azure"
    LOCAL = "local"
    GCP = "gcp"
    S3 = "s3"
