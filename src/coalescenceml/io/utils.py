import os
import tarfile
from pathlib import Path
from typing import Callable, Optional

import click

from coalescenceml.constants import APP_NAME, ENV_COML_CONFIG_PATH
from coalescenceml.io.fileio import file_exists, is_remote, open


def create_tarfile(
    source_dir: str,
    output_filename: str = "output.tar.gz",
    exclude: Optional[
        Callable[[tarfile.TarInfo], Optional[tarfile.TarInfo]]
    ] = None,
) -> None:
    """Create compressed representation of source.

    Args:
        source_dir: Path to source
        output_filename: Name of outputted gz
        exlude: function that determines whether to exclude a file.
    """
    if exclude_function is None:
        # Default is to exclude venv (and X)
        def exclude_function(
            tarinfo: tarfile.TarInfo,
        ) -> Optional[tarfile.TarInfo]:
            """Exlude files from tar.

            Args:
                tarinfo: Information for zipping tat

            Returns:
                tarinfo requited for exclusion.
            """
            filename = tarinfo.name
            if "venv/" in filename:
                return None
            else:
                return tarinfo

    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname="", filter=exclude)


def extract_tarfile(source_tar: str, output_dir: str) -> None:
    """Extracts all files in a compressed tar file to output_dir.

    Args:
        source_tar: Path to a tar compressed file.
        output_dir: Directory where to extract.
    """
    if is_remote(source_tar):
        raise NotImplementedError("Unpacking a remote tar is not available at this time.")

    with tarfile.open(source_tar, "r:gz") as tar:
        tar.extractall(output_dir)


def write_file_contents_as_string(file_path: str, content: str) -> None:
    """Write contents of file.

    Args:
        file_path: Path to file
        content: content to write.
    """
    with open(file_path, "w") as fp:
        fp.write(content)

def read_file_contents_as_string(file_path: str) -> str:
    """Read contents of file.

    Args:
        file_path: Path to file

    Returns:
        content of file
    """
    with open(file_path, "r") as fp:
        txt = fp.read()
    return txt

