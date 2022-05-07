from ast import Tuple
from pathlib import Path
from s3fs import S3FileSystem

from coalescenceml.artifact_store.base_artifact_store import BaseArtifactStore, PathType
from coalescenceml.integrations.constants import AWS_ENDPOINT_STR, AWS_ENDPOINT_URL


from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
)

class AWSArtifactStore(BaseArtifactStore):
    """ Artifact Store for AWS S3""" 

    # Class Configuration
    s3fs: Optional[S3FileSystem] = None
    region: Optional[str] = None

    @property 
    def filesystem(self):
        if not self.s3fs:
            if region:
                self.s3fs = S3FileSystem(client_kwargs={"endpoint_url": AWS_ENDPOINT_STR + region + AWS_ENDPOINT_URL})
            else:
                self.s3fs = S3FileSystem()
        return self.s3fs

    def open(name: PathType) -> Any:
        """Open a file at the given path."""
        return self.s3fs.open(name)

    def copyfile(src: PathType, dst: PathType, overwrite: bool = False) -> None:
        """Copy a file from the source to the destination."""
        self.s3fs.copy(src,dst)
        
    def exists(path: PathType) -> bool:
        """Returns `True` if the given path exists."""
        return self.s3fs.exists(path)

    def glob(pattern: PathType) -> List[PathType]:
        """Return the paths that match a glob pattern."""
        return self.s3fs.glob(pattern)

    def isdir(path: PathType) -> bool:
        """Returns whether the given path points to a directory."""
        return self.s3fs.isdir(path)

    def listdir(path: PathType) -> List[PathType]:
        """Returns a list of files under a given directory in the filesystem."""
        return self.s3fs.ls(path)

    def makedirs(path: PathType) -> None:
        """Make a directory at the given path, recursively creating parents."""
        self.s3fs.mkdir(path, createParents = False)

    def mkdir(path: PathType) -> None:
        """Make a directory at the given path; parent directory must exist."""
        self.s3fs.mkdir(path)

    def remove(path: PathType) -> None:
        """Remove the file at the given path. Dangerous operation."""
        self.s3fs.rm(path)

    def rename(src: PathType, dst: PathType, overwrite: bool = False) -> None:
        """Rename source file to destination file.
        Args:
            src: The path of the file to rename.
            dst: The path to rename the source file to.
            overwrite: If a file already exists at the destination, this
                method will overwrite it if overwrite=`True`
        """
        if not overwrite and self.exists(dst):
            raise FileExistsError(
                f"Destination path {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        self.s3fs.rename(src, dst)

    def rmtree(path: PathType) -> None:
        """Deletes dir recursively. Dangerous operation."""
        self.s3fs.rmdir(path)

    def stat(path: PathType) -> Any:
        """Return the stat descriptor for a given file path."""
        return self.s3fs.info(path)

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
        iter = self.s3fs.walk(top)	
        if topdown:
            iter
        else:
            iter.reverse()

 
