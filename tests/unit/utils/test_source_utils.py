import inspect
import os
import sys
from contextlib import ExitStack as does_not_raise

import pytest

from coalescenceml.directory import Directory
from coalescenceml.utils import source_utils


def test_is_third_party_module():
    """Test that third party module gets detected peoperly."""
    third_party_file = inspect.getfile(pytest.Cache)
    assert source_utils.is_third_party_module(third_party_file)


def test_get_source():
    """Test is source is gotten properly"""
    assert source_utils.get_source(pytest.Cache)


def test_get_hashed_source():
    """Test if hash of source is computed properly."""
    assert source_utils.get_hashed_source(pytest.Cache)


def test_prepend_path():
    """Test the context manager prepends an element to the pythonpath and removes it again after the context is exited."""
    path_element = "not_on_path"
    
    assert path_element not in sys.path
    with source_utils.prepend_python_path(path_element):
        assert sys.path[0] == path_element

    assert path_element not in sys.path
