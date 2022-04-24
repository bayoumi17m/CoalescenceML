# import fnmatch
# import os
# from pathlib import Path
# from typing import Any, Callable, Iterable, List, Optional, Tuple, Type

# from tfx.dsl.io.filesystem import Filesystem, PathType

# from coalescenceml.constants import REMOTE_FS_PREFIX
# from coalescenceml.io.fileio_registry import fileio_registry
# from coalescenceml.logger import get_logger


# logger = get_logger(__name__)


# def pathtype_to_str(path: PathType) -> str:
#     """Converts PathType to a str.

#     Args:
#         path: location of file.

#     Returns:
#         location of file as a string.
#     """
#     if isinstance(path, str):
#         return path
#     else:
#         return path.decode("utf-8")


# def get_filesystem(path: PathType) -> Type[Filesystem]:
#     """Returns an abstract filesystem class for a given path from the registry.

#     Args:
#         path: path to find associated filesystem for

#     Returns:
#         Filesystem associated with path
#     """
#     return fileio_registry.get_filesystem_for_path(path)


# def open(path: PathType, mode: str = "r") -> Any: # Fix this to be some type of stream.
#     """Open a file at a given path.

#     Args:
#         path: location of file to open.
#         mode: How to open the file [r, w, a]

#     Returns:
#         file stream to operate on.
#     """
#     return get_filesystem(path).open(path, mode=mode)


# def copy(src: PathType, dst: PathType, overwrite: bool = False) -> None:
#     """Copy a file from source to destination.

#     Args:
#         src: source location to grab file.
#         dst: destination location to store file.
#         overwrite: whether to overwrite file if it exists.

#     Raises:
#         FileExistsError: if overwrite is false and file exists in destination
#     """
#     src_fs = get_filesystem(src)
#     tgt_fs = get_filesystem(tgt)

#     if src_fs is dst_fs:
#         src_fs.copy(src, dst, overwrite=overwrite)
#     else:
#         if file_exists(dst) and not overwrite:
#             raise FileExistsError(f"File {pathtype_to_str} in destination already exists and overwrite is false.")
#         with open(src, mode="rb") as fp:
#             contents = fp.read()
#         with open(dst, mode="wb") as fp:
#             fp.write(contents)


# def file_exists(path: PathType) -> bool:
#     """Return True if the given path exists.

#     Args:
#         path: location of file

#     Returns:
#         whether file exists at location.
#     """
#     return get_filesystem(path).exists(path)


# def remove(path: PathType) -> None:
#     """Remove the file at the given path.

#     Args:
#         path: location of file

#     Warning:
#         Dangerous operation! Do at your own peril...

#     Raises:
#         FileNotFoundError: path does not exist
#     """
#     if not file_exists(path):
#         raise FileNotFoundError(f"{pathtype_to_str(path)} does not exist.")
#     get_filesystem(path).remove(path)


# def is_dir(path: PathType) -> bool:
#     """Returns whether path is a directory.

#     Args:
#         path: location of file

#     Returns:
#         True is the path is a directory, otherwise False
#     """
#     return get_filesystem(path).isdir(path)


# def glob(pattern: PathType) -> List[PathType]:
#     """Return paths that match the pattern.

#     Args:
#         patten: expression of paths with wildcards

#     Returns:
#         List of paths that match the pattern
#     """
#     return get_filesystem(path).glob(path)


# def list_dir(dir_path: str, only_file_names: bool = False) -> List[str]:
#     """Returns a list of files in dir.

#     Args:
#         dir_path: Path in filesystem.
#         only_file_names: Returns only file names if True.

#     Returns:
#         List of paths in dir
#     """
#     try:
#         return [
#             pathtype_to_str(f)
#             if only_file_names
#             else os.path.join(dir_path, pathtype_to_str(f))
#             for f in get_filesystem(dir_path).listdir(dir_path)
#         ]
#     except IOError:
#         raise ValueError(f"Dir {dir_path} not found.")


# def make_dirs(path: PathType) -> None:
#     """Make a directory at path, creating parents.

#     Args:
#         path: Path in filesystem to make
#     """
#     get_filesystem(path).makedirs(path)


# def mkdir(path: PathType) -> None:
#     """Make a directory at path, parent directory must exist.

#     Args:
#         path: Path in filesystem to make
#     """
#     get_filesystem(path).mkdir(path)


# def rename(src: PathType, dst: PathType, overwrite: bool = True) -> None:
#     """Rename source file to destination file.

#     Args:
#         src: Path of file to rename.
#         dst: Path of file to rename to.
#         overwrite: If file exists at destination, then it will be overwritten if this is True and raise a FileExistsError otherwise

#     Raises:
#         FileExistsError: If file already exists at the destination and overwrite is set to False.
#         ValueError: If src and dst use different filesystems plugins
#     """
#     src_fs = get_filesystem(src)
#     dst_fs = get_filesystem(dst)
#     if src_fs is dst_fs:
#         src_fs.rename(src, dst, overwrite=overwrite)
#     else:
#         raise ValueError(f"Renaming from different plugins is not supported.")


# def rm_dir(dir_path: str) -> None:
#     """Deletes dir recursively.

#     Args:
#         dir_path: Dir to delete

#     Raises:
#         ValueError: If the path is not pointing to a directory

#     Warnings:
#         Dangerous operation....proceed with causion.
#     """
#     if not is_dir(dir_path):
#         raise ValueError(f"Path '{dir_path}' is not a directory.")

#     get_filesystem(dir_path).rmtree(dir_path)


# def stat(path: PathType) -> Any:
#     """Return stat description for given path.

#     Args:
#         path: Path in filesystem.

#     Returns:
#         stat description of file path
#     """
#     get_filesystem(path).stat(path)


# def walk(root: PathType, topdown: bool = True, onerror: Optional[Callable[..., None]] = None,) -> Iterable[Tuple[PathType, List[PathType], List[PathType]]]:
#     """Return iterator that walks contents of directory.

#     Args:
#         root: Path of directory to walk.
#         topdown: whether to walk directories topdown or bottom-up.
#         onerror: Callable that is run if error occurs.

#     Returns:
#         Iterable of tuples which contain: Path of the current directory path, a list of directories inside the current directory, and a list of files inside the current directory.
#     """
#     return get_filesystem(root).walk(root, topdown=topdown, onerror=onerror)


# def grep_files(dir_path: PathType, pattern: str) -> Iterable[str]:
#     """Find files in directory that match a pattern.

#     Args:
#         dir_path: Path to directory.
#         pattern: file path pattern like *.png

#     Yields:
#         All matching filenames
#     """
#     for root, dirs, files, in walk(dir_path):
#         for basename in files:
#             if fnmatch.fnmatch(convert_to_str(basename), pattern):
#                 filename = os.path.join(pathtype_to_str(root), pathtype_to_str(basename))
#                 yield filename


# def is_remote(path: str) -> bool:
#     """Returns True if path is remote.

#     Args:
#         path: file path in a filesystem as a string

#     Returns:
#         True if remote path else False
#     """
#     return any(path.startswith(prefix) for prefix in REMOTE_FS_PREFIX)


# def create_dir_if_not_exists(dir_path: str) -> None:
#     """Creates directory if it does not exist.

#     Args:
#         dir_path: Local path in filesystem.
#     """
#     if not is_dir(dir_path):
#         make_dirs(dir_path)


# def create_dir_recursive_if_not_exists(dir_path: str) -> None:
#     """Creates directory recursively if it does not exist.

#     Args:
#         dir_path: Local path in filesystem.
#     """
#     if not is_dir(dir_path):
#         make_dirs(dir_path)


# def resolve_relative_path(path: str) -> str:
#     """Takes a relative path and conbverts into an absolute path.

#     Args:
#         path: Local path in filesystem.

#     Returns:
#         Resolved path.
#     """
#     if is_remote(path):
#         return path
#     else:
#         return str(Path(path).resolve())

# def move(src: str, dst: str, overwrite: bool = False) -> None:
#     """Moves dir or file from source to destination.

#     Args:
#         src: Local path to copy from.
#         dst: Local path to copy to.
#         overwrite: boolean, if false, then throws an error before overwriting.
#     """
#     rename(src, dst, overwrite)


# def copy_dir(src_dir: str, dst_dir, overwrite: bool=False) -> None:
#     """Copies dir from src to dst.

#     Args:
#         src_dir: Path to copy folder from.
#         dst_dir: Path to copy folder to.
#         overwrite: whether to overwrite existing dst. If false, throws an error before overwriting.
#     """
#     for src_file in list_dir(src_dir):
#         src_file_path = Path(src_file)
#         dst_name = os.path.join(dst_dir, src_file_path.name)
#         if is_dir(src_file):
#             copy_dir(src_file, dst_name, overwrite)
#         else:
#             create_dir_recursive_if_not_exists(
#                 str(Path(dst_name).parent)
#             )
#             copy(str(src_file_path), str(dst_name), overwrite)


# def get_parent(dir_path: str) -> str:
#     """Get parent of dir.

#     Args:
#         dir_path: Path to directory.

#     Returns:
#         Parent of the dir as a string.
#     """
#     return Path(dir_path).parent.stem

from tfx.dsl.io.fileio import (  # noqa
    copy,
    exists,
    glob,
    isdir,
    listdir,
    makedirs,
    mkdir,
    open,
    remove,
    rename,
    rmtree,
    stat,
    walk,
)


__all__ = [
    "copy",
    "exists",
    "glob",
    "isdir",
    "listdir",
    "makedirs",
    "mkdir",
    "open",
    "remove",
    "rename",
    "rmtree",
    "stat",
    "walk",
]
