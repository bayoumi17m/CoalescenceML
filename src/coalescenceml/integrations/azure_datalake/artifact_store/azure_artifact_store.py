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
    Tuple
)

class AzureArtifactStore(BaseArtifactStore):
    """ Artifact Store for Azure Datalake artifacts""" 

    account_name: str
    abfs: Optional[AzureBlobFileSystem] = None

    @account_name.setter
    def set_acc_name(self, account_name: str):
        self.account_name = account_name
    
    @property 
    def filesystem(self):
        if not self.abfs:
            self.abfs = AzureBlobFileSystem(account_name=self.account_name, anon=False)
        return self.abfs

    def open(self, name: PathType, mode: str = "rb") -> Any:
        """ Open a file at""" 
        return self.abfs.open(name, mode=mode)
    
    def copyfile(self, src: PathType, dst: PathType, overwrite: bool = False) -> None:
        if not overwrite and self.exists(dst):
            raise FileExistsError(
                f"Destination file {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        return self.abfs.cp_file(src, dst) 

    def exists(self, path: PathType) -> bool:
        return self.abfs.exists(path)

    def glob(self, pattern: PathType) -> List[PathType]:
        return self.abfs.glob(pattern) 
    
    def isdir(self, path: PathType) -> bool:
        return self.abfs.isdir(path) 
     
    def listdir(self, path: PathType) -> List[PathType]:
        return self.abfs.ls(path)

    def makedirs(self, path: PathType) -> None:
        return self.abfs.mkdir(path)
    
    def mkdir(self, path: PathType) -> None:
        return self.abfs.mkdir(path, create_parents = False)

    def remove(self, path: PathType) -> None:
        return self.abfs.rm_file(path)
    
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
        return self.abfs.rm(path) 

    def stat(self, path: PathType) -> Any:
        return self.abfs.info(path)

    def walk(self, top: PathType, topdown: bool = True, onerror: Optional[Callable[..., None]] = None) -> Iterable[Tuple[PathType, List[PathType], List[PathType]]]:
        iter = self.abfs.walk(top)
        if topdown:
            return iter 
        else: 
            return iter.reverse()
