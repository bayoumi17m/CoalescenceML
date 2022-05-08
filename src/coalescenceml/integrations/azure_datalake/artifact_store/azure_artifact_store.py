import os
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    Optional,
    Set,
    Tuple
)

from adlfs import AzureBlobFileSystem
from azure.identity.aio import DefaultAzureCredential

from coalescenceml.artifact_store.base_artifact_store import BaseArtifactStore, PathType
from coalescenceml.integrations.constants import AZURE
from coalescenceml.stack.stack_component_class_registry import (
    register_stack_component_class
)


@register_stack_component_class
class AzureArtifactStore(BaseArtifactStore):
    """ Artifact Store for Azure Datalake artifacts""" 

    account_name: str
    _abfs: Optional[AzureBlobFileSystem] = None

    FLAVOR: ClassVar[str] = AZURE
    SUPPORTED_SCHEMES: ClassVar[Set[str]] = {"az://", "abfs://"}
    
    @property 
    def filesystem(self):
        if not self._abfs:
            creds = DefaultAzureCredential()
            self._abfs = AzureBlobFileSystem(account_name=self.account_name, credential = creds, anon = False)
        return self._abfs

    def open(self, name: PathType, mode: str = "rb") -> Any:
        """ Open a file at""" 
        return self.filesystem.open(name, mode=mode)
    
    def copyfile(self, src: PathType, dst: PathType, overwrite: bool = False) -> None:
        if not overwrite and self.exists(dst):
            raise FileExistsError(
                f"Destination file {str(dst)} already exists and argument "
                f"`overwrite` is false."
            )
        return self.filesystem.cp_file(src, dst) 

    def exists(self, path: PathType) -> bool:
        return self.filesystem.exists(path)

    def glob(self, pattern: PathType) -> List[PathType]:
        return self.filesystem.glob(pattern) 
    
    def isdir(self, path: PathType) -> bool:
        return self.filesystem.isdir(path) 
     
    def listdir(self, path: PathType) -> List[PathType]:
        return self.filesystem.ls(path)

    def makedirs(self, path: PathType) -> None:
        return self.filesystem.mkdir(path)
    
    def mkdir(self, path: PathType) -> None:
        return self.filesystem.mkdir(path, create_parents = False)

    def remove(self, path: PathType) -> None:
        return self.filesystem.rm_file(path)
    
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
        return self.filesystem.rm(path)

    def stat(self, path: PathType) -> Any:
        return self.filesystem.info(path)

    def walk(self, top: PathType, topdown: bool = True, onerror: Optional[Callable[..., None]] = None) -> Iterable[Tuple[PathType, List[PathType], List[PathType]]]:
        it = self.filesystem.walk(top)
        if topdown:
            it = list(it)
        else: 
            it = reversed(list(it))
