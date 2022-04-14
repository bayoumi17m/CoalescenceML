import json
from pathlib import Path
from typing import Any, Dict

import coalescenceml.io.utils
from coalescenceml.io import fileio


def write_json(file_path: str, contents: Dict[str, Any]) -> None:
    """Write contents as JSON to file_path.

    Args:
        file_path: Path to JSON file.
        contents: Contents of json as dict.

    Raises:
        FileNotFoundError: if parent directory of  file_path does not exist
    """
    if not fileio.is_remote(file_path):
        # If it is a local path
        directory = str(Path(file_path).parent)
        if not fileio.is_dir(directory):
            # If it doesn't exist, then raise exception
            raise FileNotFoundError(f"Directory '{directory}' does not exist")

    coalescenceml.io.utils.write_file_contents_as_string(
        file_path, json.dumps(contents)
    )


def read_json(file_path: str) -> Dict[str, Any]:
    """Read JSON at file path and return contents.

    Args:
        file_path: path to JSON file.

    Returns:
        Contents of the file as a dict

    Raises:
        FileNotFoundError: if file path does not exist
    """
    if fileio.file_exists(file_path):
        contents = coalescenceml.io.utils.read_file_contents_as_string(file_path)
        return json.loads(contents)
    else:
        raise FileNotFoundError(f"File '{file_path}' does not exist")

