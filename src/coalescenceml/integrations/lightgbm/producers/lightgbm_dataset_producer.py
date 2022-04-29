import os
import tempfile
from typing import Any, Type

import lightgbm as lgb

from coalescenceml.logger import get_logger
from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "data.lgb.binary"


@register_producer_class
class LightGBMDatasetProducer(BaseProducer):
    """Producer to read and write from lightgbm.Dataset."""

    TYPES = (lgb.Dataset,)
    ARTIFACT_TYPES = (DataArtifact,)

    def handle_input(self, data_type: Type[Any]) -> lgb.Dataset:
        """Read LGB Dataset data from binary file.

        Args:
            data_type: Data type to be processed.

        Returns:
            LGB Dataset data
        """
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir2 = tempfile.mkdtemp()
            # Create temp file
            temp_file = os.path.join(temp_dir, DEFAULT_FILENAME)
            # Copy from artifact store to temp file
            fileio.copy(filepath, temp_file)
            # Force eager execution to read file
            matrix = lgb.Dataset(temp_file, free_raw_data=False, params={"verbose": -1}).construct()

        logger.info(f"Matrix handle: {matrix.handle}")
        logger.info(f"Data is none? {matrix.data is None}")

        return matrix

    def handle_return(self, matrix: lgb.Dataset) -> None:
        """Create binary data file for Dataset.

        Args:
            matrix: A lightgbm dataset
        """
        super().handle_return(matrix)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Make temp artifact
        with tempfile.TemporaryDirectory() as temp_dir:
            # create temp file
            temp_file = os.path.join(temp_dir, DEFAULT_FILENAME)
            matrix.save_binary(temp_file)

            # copy file
            fileio.copy(temp_file, filepath)
