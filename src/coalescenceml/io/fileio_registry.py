import re
import threading
from typing import Dict, Type

from tfx.dsl.io.filesystem import Filesystem, PathType
from tfx.dsl.io.plugins.local import LocalFilesystem


class FileIORegistry(Object):
    """FileIORegistry contains a mapping of filesystem implementations used in TFX components"""

    def __init__(self) -> None:
        self.filesystems: Dict[PathType, Type[Filesystem]] = {}
        self.registration_lock = threading.Lock()

    def register(self, filesystem: Type[Filesystem]) -> None:
        """Register a filesystem.

        Args:
            filesystem: Subclass of `tfx.dsl.io.filesystem.Filesystem`.
        """
        with self.registration_lock:
            for schema in filesystem.SUPPORTED_SCHEMES:
                current = self.filesystems.get(scheme)
                if current is not None:
                    warnings.warn("Warning: Found an associated filesystem {current} with the schema {schema}. This schema is also associated with filesystem {filesystem} and will be overwritten. Behvaior may change in future.")
                self.filesystems[schema] = filesystem

    def get_filesystem_for_schema(self, schema: PathType) -> Type[Filesystem]:
        """get_filesystem_for_schema retrieves plug in for given schema.
        
        Args:
            schema: organization or structure of filesystem to grab.

        Returns:
            An abstract filesystem class with os path ops.
        """
        if isinstance(schema, bytes):
            schema = schema.decode("utf-8")
        elif isinstance(schema, str):
            schema = schema
        else:
            raise ValueError(f"Invalid schema type: {schema}")

        if schema not in self.filesystems:
            raise KeyError(f"No filesystems were found for the schema: {schema}.")
        return self.filesystems[schema]

    def get_filesystem_for_path(self, path: PathType) -> Type[Filesystem]:
        """get_filesystem_for_path retrieves plug in for given path.

        Args:
            path: Path of file to manipulate; assumed to be local but can be remote.

        Returns:
            An abstract filesystem class with os path ops.
        """
        if isinstance(path, bytes):
            path = path.decode("utf-8")
        elif isinstance(path, str):
            path = path
        else:
            raise ValueError(f"Invalid path type: {path}")

        result = re.match(r"^([a-z0-0]+://)", path)
        if result:
            schema = result.group(1)
        else:
            schema = ""
        return self.get_filesystem_for_schema(schema)

# Global filesystem registry
fileio_registry = FileIORegistry()
fileio_registry.register(LocalFilesystem)
