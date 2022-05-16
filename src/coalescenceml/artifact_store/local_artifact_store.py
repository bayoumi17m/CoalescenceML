import glob
import os
import shutil
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from pydantic import validator

from coalescenceml.artifact_store import BaseArtifactStore
from coalescenceml.artifact_store.exceptions import ArtifactStoreInterfaceError


# TODO: Can we have this be imported from somewhere b/c we use it a lot??
PathType = Union[bytes, str]


class LocalArtifactStore(BaseArtifactStore):
    """Artifact Store for local artifacts."""

    # Class Configuration
    FLAVOR: ClassVar[str] = "local"
    SUPPORTED_SCHEMES: ClassVar[Set[str]] = {"", "file://"}

    @staticmethod
    def open(name: PathType, mode: str = "r") -> Any:
        """Open a file at the given path."""
        return open(name, mode=mode)

    @staticmethod
    def copyfile(src: PathType, dst: PathType, overwrite: bool = False) -> None:
        """Copy a file from the source to the destination."""
        if not overwrite and os.path.exists(dst):
            raise FileExistsError(
                f"Destination file {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        shutil.copyfile(src, dst)

    @staticmethod
    def exists(path: PathType) -> bool:
        """Returns `True` if the given path exists."""
        return os.path.exists(path)

    @staticmethod
    def glob(pattern: PathType) -> List[PathType]:
        """Return the paths that match a glob pattern."""
        return glob.glob(pattern)

    @staticmethod
    def isdir(path: PathType) -> bool:
        """Returns whether the given path points to a directory."""
        return os.path.isdir(path)

    @staticmethod
    def listdir(path: PathType) -> List[PathType]:
        """Returns a list of files under a given directory in the filesystem."""
        return os.listdir(path)

    @staticmethod
    def makedirs(path: PathType) -> None:
        """Make a directory at the given path, recursively creating parents."""
        os.makedirs(path, exist_ok=True)

    @staticmethod
    def mkdir(path: PathType) -> None:
        """Make a directory at the given path; parent directory must exist."""
        os.mkdir(path)

    @staticmethod
    def remove(path: PathType) -> None:
        """Remove the file at the given path. Dangerous operation."""
        os.remove(path)

    @staticmethod
    def rename(src: PathType, dst: PathType, overwrite: bool = False) -> None:
        """Rename source file to destination file.
        Args:
            src: The path of the file to rename.
            dst: The path to rename the source file to.
            overwrite: If a file already exists at the destination, this
                method will overwrite it if overwrite=`True`
        """
        if not overwrite and os.path.exists(dst):
            raise FileExistsError(
                f"Destination path {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        os.rename(src, dst)

    @staticmethod
    def rmtree(path: PathType) -> None:
        """Deletes dir recursively. Dangerous operation."""
        shutil.rmtree(path)

    @staticmethod
    def stat(path: PathType) -> Any:
        """Return the stat descriptor for a given file path."""
        return os.stat(path)

    @staticmethod
    def walk(
        top: PathType,
        topdown: bool = True,
        onerror: Optional[Callable[..., None]] = None,
    ) -> Iterable[Tuple[PathType, List[PathType], List[PathType]]]:
        """Return an iterator that walks the contents of the given directory.
        Args:
            top: Path of directory to walk.
            topdown: Whether to walk directories topdown or bottom-up.
            onerror: Callable that gets called if an error occurs.
        Returns:
            An Iterable of Tuples, each of which contain the path of the
            current directory path, a list of directories inside the
            current directory and a list of files inside the current
            directory.
        """
        yield from os.walk(top, topdown=topdown, onerror=onerror)

    @validator("path")
    def ensure_path_local(cls, path: str) -> str:
        from coalescenceml.constants import REMOTE_FS_PREFIX

        if any(path.startswith(prefix) for prefix in REMOTE_FS_PREFIX):
            raise ArtifactStoreInterfaceError(
                f"The path:{path} you defined for your local artifact store "
                f"start with one of the remote prefixes."
            )
        return path
