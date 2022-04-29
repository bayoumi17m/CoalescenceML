import json
import os
from typing import AbstractSet, Any, Dict, Iterable, List, Optional, cast

import pkg_resources
from docker.client import DockerClient
from docker.utils import build as docker_build

import coalescenceml
from coalescenceml import __version__
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import ENV_COML_CONFIG_PATH
from coalescenceml.io import fileio
from coalescenceml.io.utils import read_file_contents_as_string
from coalescenceml.logger import get_logger
from coalescenceml.utils import readability_utils

DEFAULT_BASE_IMAGE = "python:3.8.13-slim-buster"
CUDA_BASE_IMAGE = "tensorflow/tensorflow:2.6.1-gpu"
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
    base_image: str,
    entrypoint: Optional[str] = None,
    requirements: Optional[AbstractSet[str]]= None,
    environment_vars: Optional[Dict[str, str]] = None,
) -> str:
    """Generate a Dockerfile.

    Args:
        base_image: The image to use as a base for the dockerfile.
        entrypoint: The default entrypoint command that is to be executed
            when running the container.
        requirements: List of pip requirements to install.
        environment_vars: Dict of environment variables to set.

    Returns:
        contents of Dockerfile
    """
    lines = [f"from {base_image}", "WORKDIR /app"]

    if environment_vars:
        for key, value in environment_vars.items():
            lines.append(f"ENV {key}={value}")

    lines.append(f"RUN pip install --no-cache coalescenceml=={__version__}")
    if requirements:
        lines.append(
            f"RUN pip install --no-cache {' '.join(sorted(requirements))}"
        )

    lines.append("COPY . .")
    lines.append("RUN chmod -R a+rw .")
    lines.append(f"ENV {ENV_COML_CONFIG_PATH}=/app/{CONTAINER_COML_CONFIG_DIR}")

    if entrypoint:
        lines.append(f"ENTRYPOINT {entrypoint}")

    return "\n".join(lines)


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

def process_docker_stream(stream: Iterable[bytes]) -> None:
    """Process output stream of docker command.

    Raises:
        JSONDecodeError: If an element of the stream is not json decodable.
        RuntimeError: If there was an error while running the docker command.
    """
    for element in stream:
        lines = element.decode("utf-8").strip().split("\n")

        for line in lines:
            try:
                line_json = json.loads(line)
                if "error" in line_json:
                    raise RuntimeError(f"Docker Error: {line_json['error']}")
                elif "stream" in line_json:
                    logger.info(line_json['stream'].strip())
                else:
                    pass
            except json.JSONDecodeError as error:
                logger.warning(
                    f"Failed to decode json for line '{line}' with "
                    f"error: {error}"
                )


def push_docker_image(image_name: str) -> None:
    """Push a docker image to registry.

    Args:
        image_name: The name and tag of the image to push.
    """
    logger.info(f"Pushing docker image '{image_name}'")
    docker_client = DockerClient.from_env()
    process_docker_stream(output_stream)


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
    config_path = os.path.join(build_context_path, CONTAINER_COML_CONFIG_DIR)
    try:
        # TODO: Find a cleaner way than copying the literal profiles and
        # stack into the docker container. If we happen to store secrets
        # then those will be stored in our container....
        # Save global config with active profile and stack in build context
        # so that they are accessible within the container
        GlobalConfiguration().copy_active_configuration(
            config_path,
            load_config_path=f"/app/{CONTAINER_COML_CONFIG_DIR}",
        )

        if not requirements and use_local_requirements:
            local_requirements = get_current_environment_requirements()
            requirements = {
                f"{package}=={version}"
                for package, version in local_requirements.items()
                if package != "coalescenceml"
            }
            logger.info(
                "Using requirements from local environment for docker "
                "image: {requirements}"
            )
        elif requirements and use_local_requirements:
            logger.warning(
                "Using requirements passed in and local requirements. "
                "This has less defined behavior and may be changed in the "
                "future. There are no guarantees your dependencies resolve."
            )
            local_requirements = get_current_environment_requirements()
            requirements = requirements.union(
                {
                    f"{package}=={version}"
                    for package, version in local_requirements.items()
                    if package != "coalescenceml"
                }
            )

        if dockerfile_path:
            dockerfile_contents = read_file_contents_as_string(dockerfile_path)
        else:
            dockerfile_contents = generate_dockerfile_contents(
                base_image=base_image or DEFAULT_BASE_IMAGE,
                entrypoint=entrypoint,
                requirements=requirements,
                environment_vars=environment_vars,
            )

        build_context = create_build_context(
            build_context_path=build_context_path,
            dockerfile_contents=dockerfile_contents,
            dockerignore_path=dockerignore_path,
        )

        logger.info(
            f"Building docker image '{image_name}'"
        )

        docker_client = DockerClient.from_env()
        output_stream = docker_client.images.client.api.build(
            fileobj=build_context,
            custom_context=True,
            tag=image_name,
        )

        process_docker_stream(output_stream)
    finally:
        # Clean temporary build files
        fileio.rmtree(config_path)

    logger.info("Finished building docker image.")

def get_image_digest(image_name: str) -> Optional[str]:
    docker_client = DockerClient.from_env()
    image = docker_client.images.get(image_name)
    repo_digests = image.attrs["RepoDigests"]
    if len(repo_digests) == 1:
        return cast(str, repo_digests[0])
    else:
        logger.debug(
            "Found 0 or 2+ repo digests for the image "
            f"'{image_name}': {repo_digests}"
        )
