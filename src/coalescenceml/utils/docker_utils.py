import json
import os
from typing import AbstractSet, Any, Dict, Iterable, List, Optional, cast

import pkg_resources
from docker.client import DockerClient
from docker.utils import build as docker_build

import coalescenceml
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import ENV_COML_CONFIG_PATH
from coalescenceml.io import fileio
from coalescenceml.io.utils import read_file_contents_as_string
from coalescenceml.logger import get_logger
from coalescenceml.utils import readability_utils

DEFAULT_BASE_IMAGE = "python:3.8.13-slim-buster"
CONTAINER_COML_CONFIG_DIR = ".coalescenceconfig"

logger = get_logger(__name__)


def parse_dockerignore(dockerignore_path: str) -> List[str]:
    """Parse a dockerignore and return list of patterns.

    Args:
        dockerignore_path: location of dockerignore

    Returns:
        list of patterns to match
    """
    try:
        file_content = read_file_contents_as_string(dockerignore_path)
    except FileNotFoundError:
        logger.warning(
            f"Unable to find dockerignore file at path '{dockerignore_path}'"
        )
        return []

    exclude_patterns = []
    for line in file_content.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            exclude_pattens.append(line)

    return exclude patterns


def generate_dockerfile_contents(
) -> str:
    return ""


def create_build_context(
    build_context_path: str,
    dockerfile_contents: str,
    dockerignore_path: Optional[str] = None,
) -> Any:
    default_dockerignore_path = os.path.join(
        build_context_path, ".dockerignore"
    )

    if dockerignore_path:
        exclude_patterns = parse_dockerignore(dockerignore_path)
    elif fileio.exists(default_dockerignore_path):
        exclude_patterns = parse_dockerignore(default_dockerignore_path)
    else:
        exclude_patterns = []


    logger.debug(
        f"Exclude patterns for docker build context: {exclude_patterns}"
    )

    files = docker_build.exclude_paths(
        build_context_path, patterns=exclude_patterns
    )

    context = docker_build.create_archive(
        root=build_context_path,
        files=sorted(files),
        gzip=False,
        extra_files={
            "Dockerfile": dockerfile_contents,
        }
    )

    build_context_size = os.path.getsize(context.name)
    filesize_50mb = (1024 * 1024) * 50
    if build_context_size > filesize_50mb:
        # Build context exceeds 50mb; Make sure to ignore things!
        logger.warning(
            f"Build context size for docker image: "
            f"{readability_utils.get_human_readable_filesize}. If you believe "
            f"this is large, make sure to include a `.dockerignore` file at "
            f"the root of your build context '{build_context_path}' or specify "
            f"a custom file when defining your pipeline."
        )

    return context


def get_current_envionment_requirements() -> Dict[str, str]:
    """Return package requirements for current python process.

    Returns:
        A dict of package requirements for current environment
    """
    return {
        distribution.key: distribution.version
        for distribution in pkg_resources.working_set
    }


def build_docker_image(
    build_context_path: str,
    image_name: str,
    entrypoint: Optional[str] =None,
    dockerfile_path: Optional[str] = None,
    dockerignore_path: Optional[str] = None,
    requirements: Optional[AbstractSet[str]] = None,
    environment_vars: Optional[Dict[str, str]] = None,
    use_local_requirements: bool =False,
    base_image: Optional[str] = None,
) -> None:
    """Build a docker image.

    Args:
    """
    pass
