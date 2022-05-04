from ast import Tuple
import os
from pathlib import Path
from adlfs import AzureBlobFileSystem

from coalescenceml.artifact_store.base_artifact_store import BaseArtifactStore, PathType
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
)

class AzureArtifactStore(BaseArtifactStore):
    """ Artifact Store for Azure Datalake artifacts""" 

    abfs: Optional[AzureBlobFileSystem] = None
    account_name: str

    def _ensure_filesystem_exists(self):
        if not self.abfs:
            self.abfs = AzureBlobFileSystem(account_name=self.account_name, anon=False)
        return self.abfs

    def open(self, name: PathType, mode: str = "rb") -> Any:
        """ Open a file at""" 
        self._ensure_filesystem_exists()
        return self.abfs._open(name, mode=mode)
    
    def copyfile(self, src: PathType, dst: PathType, overwrite: bool = False) -> None:
        if not overwrite and self.exists(dst):
            raise FileExistsError(
                f"Destination file {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        return self.abfs._cp_file(src, dst) 

    def exists(self, path: PathType) -> bool:
        return self.abfs.exists(path)

    def glob(self, pattern: PathType) -> List[PathType]:
        return self.abfs.glob(pattern) 
    
    def isdir(self, path: PathType) -> bool:
        return self.abfs.isdir(path) 
     
    def listdir(self, path: PathType) -> List[PathType]:
        return self.abfs.info(path)

    def makedirs(self, path: PathType) -> None:
        return self.abfs._mkdir(path)
    
    def mkdir(self, path: PathType) -> None:
        return self.abfs._mkdir(path, create_parents = False)

    def remove(self, path: PathType) -> None:
        return self.abfs._rm_file(path)
    
    def rename(self, src: PathType, dst: PathType, overwrite: bool = False) -> None:
        if self.exists(dst) and not overwrite:
            raise FileExistsError(
                f"Destination file {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        else:
            self.copyfile(src, dst, overwrite=overwrite) 
            self.remove(src)
            return


    def rmtree(self, path: PathType) -> None:
        return self.abfs._rm(path) 

    def stat(self, path: PathType) -> Any:
        return self.abfs._details(path)

    def walk(self, top: PathType, topdown: bool = True, onerror: Optional[Callable[..., None]] = None) -> Iterable[Tuple[PathType, List[PathType], List[PathType]]]:
        iter = self.abfs._async_walk(top)
        if topdown:
            return iter 
        else: 
            return iter.reverse()
