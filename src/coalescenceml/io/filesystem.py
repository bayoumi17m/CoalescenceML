from typing import Any, Dict, Tuple, Type, Union

from tfx.fsl.io.filesystem import Filesystem as BaseFileSystem

from coalescenceml.io.fileio_registry import fileio_registry


class Filesystem(BaseFileSystem):
    """Abstract Filesystem class."""

    def __init__(self):
        fileio_registry.register(self.__class__)
