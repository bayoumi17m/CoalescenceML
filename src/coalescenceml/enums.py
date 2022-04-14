import logging
from enum import Enum
from typing import List

class DictEnum(str, Enum):

    @classmethod
    def names(cls) -> List[str]:
        """Get all enum names as a list of strings"""
        return [c.name for c in cls]

    @classmethod
    def values(cls) -> List[str]:
        """Get all enum values as a list of strings"""
        return [c.value for c in cls]

class LoggingLevels(Enum):
    
    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARN = logging.WARN
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

class ExecutionStatus(DictEnum):

    FAILED = "failed"
    RUNNING = "running"
    CACHED = "cached"
    COMPLETED = "completed"

class StackComponentFlavor(DictEnum):
    """All possible types a `StackComponent` can have."""
    
    ORCHESTRATOR = "orchestrator"
    METADATA_STORE = "metadata_store"
    ARTIFACT_STORE = "artifact_store"
    CONTAINER_REGISTRY = "container_registry"
    STEP_OPERATOR = "step_operator" # TODO
    FEATURE_STORE = "feature_store" # TODO
    SECRETS_MANAGER = "secrets_manager" # TODO
    MODEL_DEPLOYER = "model_deployer" # TODO

class DirectoryStoreFlavor(DictEnum):
    """Directory Store Backend Types"""

    LOCAL = "local"
    SQL = "sql"

class MetadataContextFlavor(DictEnum):
    """All possible types that contexts can have within pipeline nodes"""

    STACK = "stack"
    PIPELINE_REQUIREMENTS = "pipeline_requirements"
