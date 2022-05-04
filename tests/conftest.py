import os
import shutil

import pytest
from pytest_mock import MockerFixture

from coalescenceml.directory import Directory
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import (
    ENV_COML_DEBUG,
    ENV_COML_CONFIG_PATH,
    ENV_COML_DEFAULT_STORE_TYPE,
)
from coalescenceml.pipeline import pipeline


@pytest.fixture(scope="session", autouse=True)
def base_repo(
    tmp_path_factory: pytest.TempPathFactory,
    session_mocker: MockerFixture,
    request: pytest.FixtureRequest,
):
    # Global config and Directory can't exist otherwise
    # The configuration of environment might be screwed.
    assert GlobalConfiguration.get_instance() is None
    assert Directory.get_instance() is None

    # original working directory
    orig_cwd = os.get_cwd()

    # Set env variables
    os.environ[ENV_COML_DEBUG] = "true"
    os.environ[ENV_COMLDEFAULT_STORE_TYPE] = "local"

    # change working directory to temp
    tmp_path = tmp_path_factory.mktemp("tmp")
    os.chdir(tmp_path)

    os.environ[ENV_COML_CONFIG_PATH] = str(tmp_path / "coalescenceml")

    # init directory at new path
    dir_ = Directory()

    yield dir_

    # Clean up
    os.chdir(orig_cwd)
    shutil.rmtree(tmp_path)

    # Reset global config and directory
    GlobalConfiguration._reset_instance()
    Directory._reset_instance()


@pytest.fixture
def clean_directory(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
    base_repo: Directory,
) -> Directory:
    orig_cwd = os.getcwd()
    orig_config_path = os.getenv(ENV_COML_CONFIG_PATH)

    # Change working directory to temp
    test_name = request.node.name
    test_name = test_name.replace("[", "-").replace("]", "-")
    tmp_path = tmp_path_factory.mktemp(test_name)

    os.chdir(tmp_path)

    # Save current global config and dir to restore them later
    # and reset new ones
    original_config = GlobalConfiguration.get_instance()
    original_dir = Directory.get_instance()
    GlobalConfiguration._reset_instance()
    Directory._reset_instance()

    # Set the environment variable and initialize with new tmp path
    os.environ[ENV_COML_CONFIG_PATH] = str(tmp_path / "coalescenceml")
    dir_ = Directory()

    yield dir_

    # Clean up
    os.chdir(orig_cwd)
    shutil.rmtree(tmp_path)

    os.environ[ENV_COML_CONFIG_PATH] = orig_config_path

    # Restore instances
    GlobalConfiguration._reset_instance(original_config)
    Directory._reset_instance(original_dir)



@pytest.fixture
def one_step_pipeline():
    """Pytest fixture that returns a pipeline which takes a single step
    named `step_`."""

    @pipeline
    def _pipeline(step_):
        step_()

    return _pipeline
