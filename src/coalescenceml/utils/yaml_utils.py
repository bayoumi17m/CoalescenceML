from pathlib import Path
from typing import Any, Dict

import yaml

from coalescenceml.io import fileio, utils


def write_yaml(file_path: str, contents: Dict[Any, Any]) -> None:
    """Write contents as YAML format to file_path.
    Args:
        file_path: Path to YAML file.
        contents: Contents of YAML file as dict.
    Raises:
        FileNotFoundError: if directory does not exist.
    """
    if not utils.is_remote(file_path):
        dir_ = str(Path(file_path).parent)
        if not fileio.isdir(dir_):
            raise FileNotFoundError(f"Directory {dir_} does not exist.")
    utils.write_file_contents_as_string(file_path, yaml.dump(contents))


def append_yaml(file_path: str, contents: Dict[Any, Any]) -> None:
    """Append contents to a YAML file at file_path."""
    file_contents = read_yaml(file_path) or {}
    file_contents.update(contents)
    if not utils.is_remote(file_path):
        dir_ = str(Path(file_path).parent)
        if not fileio.isdir(dir_):
            raise FileNotFoundError(f"Directory {dir_} does not exist.")
    utils.write_file_contents_as_string(file_path, yaml.dump(file_contents))


def read_yaml(file_path: str) -> Any:
    """Read YAML on file path and returns contents as dict.
    Args:
        file_path: Path to YAML file.
    Returns:
        Contents of the file in a dict.
    Raises:
        FileNotFoundError: if file does not exist.
    """

    if fileio.exists(file_path):
        contents = utils.read_file_contents_as_string(file_path)
        # TODO: [LOW] consider adding a default empty dict to be returned
        #   instead of None
        return yaml.safe_load(contents)
    else:
        raise FileNotFoundError(f"{file_path} does not exist.")


def is_yaml(file_path: str) -> bool:
    """Returns True if file_path is YAML, else False
    Args:
        file_path: Path to YAML file.
    Returns:
        True if is yaml, else False.
    """
    if file_path.endswith("yaml") or file_path.endswith("yml"):
        return True
    return False
