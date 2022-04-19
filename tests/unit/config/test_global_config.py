# import os
# from uuid import uuid4

# import pytest

# from coalescenceml.config.global_config import GlobalConfiguration
# from coalescenceml.io import fileio

# def test_global_config_file_creation(clean_repo):
#     """Tests whether a config file gets created when the global
#     config object is first instantiated."""
#     if fileio.exists(GlobalConfiguration()._config_file()):
#         fileio.remove(GlobalConfiguration()._config_file())

#     GlobalConfiguration._reset_instance()
#     assert fileio.exists(GlobalConfiguration()._config_file())


# def test_global_config_user_id_is_immutable(clean_repo):
#     """Tests that the global config user id attribute is immutable."""
#     with pytest.raises(TypeError):
#         GlobalConfiguration().user_id = uuid4()