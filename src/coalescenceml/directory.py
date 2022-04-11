import base64
import json
import os
import random
from abc import ABCMeta
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

import yaml
from pydantic import BaseModel, ValidationError

from coalescenceml.config.base_config import BaseConfiguration
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.config.profile_config import ProfileConfiguration
from coalescenceml.constants import ENV_coalescenceml_REPOSITORY_PATH, REPOSITORY_DIRECTORY_NAME
from coalescenceml.enums import StackComponentType, StoreType
from coalescenceml.environment import Environment
from coalescenceml.exceptions import (
    ForbiddenRepositoryAccessError,
    InitializationException,
)
from coalescenceml.io import fileio, utils
from coalescenceml.logger import get_logger
from coalescenceml.post_execution import PipelineView
from coalescenceml.stack import Stack, StackComponent
from coalescenceml.stack_store import (
    BaseStackStore,
    LocalStackStore,
    RestStackStore,
    SqlStackStore,
)
from coalescenceml.stack_store.models import (
    StackComponentWrapper,
    StackStoreModel,
    StackWrapper,
)
from coalescenceml.utils import yaml_utils