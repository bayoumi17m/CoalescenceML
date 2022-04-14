# import os
# import tarfile
# from pathlib import Path
# from typing import Callable, Optional

# import click

# from coalescenceml.constants import APP_NAME, ENV_COML_CONFIG_PATH
# from coalescenceml.io.fileio import file_exists, is_remote, open


# def create_tarfile(
#     source_dir: str,
#     output_filename: str = "output.tar.gz",
#     exclude: Optional[
#         Callable[[tarfile.TarInfo], Optional[tarfile.TarInfo]]
#     ] = None,
# ) -> None:
#     """Create compressed representation of source.

#     Args:
#         source_dir: Path to source
#         output_filename: Name of outputted gz
#         exlude: function that determines whether to exclude a file.
#     """
#     if exclude_function is None:
#         # Default is to exclude venv (and X)
#         def exclude_function(
#             tarinfo: tarfile.TarInfo,
#         ) -> Optional[tarfile.TarInfo]:
#             """Exlude files from tar.

#             Args:
#                 tarinfo: Information for zipping tat

#             Returns:
#                 tarinfo requited for exclusion.
#             """
#             filename = tarinfo.name
#             if "venv/" in filename:
#                 return None
#             else:
#                 return tarinfo

#     with tarfile.open(output_filename, "w:gz") as tar:
#         tar.add(source_dir, arcname="", filter=exclude)


# def extract_tarfile(source_tar: str, output_dir: str) -> None:
#     """Extracts all files in a compressed tar file to output_dir.

#     Args:
#         source_tar: Path to a tar compressed file.
#         output_dir: Directory where to extract.
#     """
#     if is_remote(source_tar):
#         raise NotImplementedError("Unpacking a remote tar is not available at this time.")

#     with tarfile.open(source_tar, "r:gz") as tar:
#         tar.extractall(output_dir)


# def write_file_contents_as_string(file_path: str, content: str) -> None:
#     """Write contents of file.

#     Args:
#         file_path: Path to file
#         content: content to write.
#     """
#     with open(file_path, "w") as fp:
#         fp.write(content)

# def read_file_contents_as_string(file_path: str) -> str:
#     """Read contents of file.

#     Args:
#         file_path: Path to file

#     Returns:
#         content of file
#     """
#     with open(file_path, "r") as fp:
#         txt = fp.read()
#     return txt

import fnmatch
import os
from pathlib import Path
from typing import Iterable

import click
from tfx.dsl.io.filesystem import PathType

from coalescenceml.constants import APP_NAME, ENV_COML_CONFIG_PATH, REMOTE_FS_PREFIX
from coalescenceml.io.fileio import (
    copy,
    exists,
    isdir,
    listdir,
    makedirs,
    mkdir,
    open,
    walk,
)


def get_global_config_directory() -> str:
    """Returns the global config directory for ZenML."""
    env_var_path = os.getenv(ENV_COML_CONFIG_PATH)
    if env_var_path:
        return str(Path(env_var_path).resolve())
    return click.get_app_dir(APP_NAME)


def write_file_contents_as_string(file_path: str, content: str) -> None:
    """Writes contents of file.
    Args:
        file_path: Path to file.
        content: Contents of file.
    """
    with open(file_path, "w") as f:
        f.write(content)


def read_file_contents_as_string(file_path: str) -> str:
    """Reads contents of file.
    Args:
        file_path: Path to file.
    """
    if not exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist!")
    return open(file_path).read()  # type: ignore[no-any-return]


def find_files(dir_path: PathType, pattern: str) -> Iterable[str]:
    """Find files in a directory that match pattern.
    Args:
        dir_path: Path to directory.
        pattern: pattern like *.png.
    Yields:
         All matching filenames if found.
    """
    for root, dirs, files in walk(dir_path):
        for basename in files:
            if fnmatch.fnmatch(convert_to_str(basename), pattern):
                filename = os.path.join(
                    convert_to_str(root), convert_to_str(basename)
                )
                yield filename


def is_remote(path: str) -> bool:
    """Returns True if path exists remotely.
    Args:
        path: Any path as a string.
    Returns:
        True if remote path, else False.
    """
    return any(path.startswith(prefix) for prefix in REMOTE_FS_PREFIX)


def create_file_if_not_exists(
    file_path: str, file_contents: str = "{}"
) -> None:
    """Creates file if it does not exist.
    Args:
        file_path: Local path in filesystem.
        file_contents: Contents of file.
    """
    full_path = Path(file_path)
    if not exists(file_path):
        create_dir_recursive_if_not_exists(str(full_path.parent))
        with open(str(full_path), "w") as f:
            f.write(file_contents)


def create_dir_if_not_exists(dir_path: str) -> None:
    """Creates directory if it does not exist.
    Args:
        dir_path: Local path in filesystem.
    """
    if not isdir(dir_path):
        mkdir(dir_path)


def create_dir_recursive_if_not_exists(dir_path: str) -> None:
    """Creates directory recursively if it does not exist.
    Args:
        dir_path: Local path in filesystem.
    """
    if not isdir(dir_path):
        makedirs(dir_path)


def resolve_relative_path(path: str) -> str:
    """Takes relative path and resolves it absolutely.
    Args:
      path: Local path in filesystem.
    Returns:
        Resolved path.
    """
    if is_remote(path):
        return path
    return str(Path(path).resolve())


def copy_dir(
    source_dir: str, destination_dir: str, overwrite: bool = False
) -> None:
    """Copies dir from source to destination.
    Args:
        source_dir: Path to copy from.
        destination_dir: Path to copy to.
        overwrite: Boolean. If false, function throws an error before overwrite.
    """
    for source_file in listdir(source_dir):
        source_path = os.path.join(source_dir, convert_to_str(source_file))
        destination_path = os.path.join(
            destination_dir, convert_to_str(source_file)
        )
        if isdir(source_path):
            if source_path == destination_dir:
                # if the destination is a subdirectory of the source, we skip
                # copying it to avoid an infinite loop.
                return
            copy_dir(source_path, destination_path, overwrite)
        else:
            create_dir_recursive_if_not_exists(
                str(Path(destination_path).parent)
            )
            copy(str(source_path), str(destination_path), overwrite)


def get_grandparent(dir_path: str) -> str:
    """Get grandparent of dir.
    Args:
        dir_path: Path to directory.
    Returns:
        The input path's parent's parent.
    """
    return Path(dir_path).parent.parent.stem


def get_parent(dir_path: str) -> str:
    """Get parent of dir.
    Args:
        dir_path: Path to directory.
    Returns:
        Parent (stem) of the dir as a string.
    """
    return Path(dir_path).parent.stem


def convert_to_str(path: PathType) -> str:
    """Converts a PathType to a str using UTF-8."""
    if isinstance(path, str):
        return path
    else:
        return path.decode("utf-8")


def is_root(path: str) -> bool:
    """Returns true if path has no parent in local filesystem.
    Args:
        path: Local path in filesystem.
    Returns:
        True if root, else False.
    """
    return Path(path).parent == Path(path)
