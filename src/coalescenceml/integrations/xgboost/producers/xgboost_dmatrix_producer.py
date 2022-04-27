import os
import tempfile
from typing import Any, Type

import xgboost as xgb

from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


DEFAULT_FILENAME = "data.xgb.binary"


@register_producer_class
class XgboostDMatrixProducer(BaseProducer):
    """Producer to read and write from xgboost.DMatrix."""

    TYPES = (xgb.DMatrix,)
    ARTIFACT_TYPES = (DataArtifact,)

    def handle_input(self, data_type: Type[Any]) -> xgb.DMatrix:
        """Read XGB DMatrix data from binary file.

        Args:
            data_type: Data type to be processed.

        Returns:
            XGB DMatrix data
        """
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create temp file
            temp_file = os.path.join(temp_dir, DEFAULT_FILENAME)
            # Copy from artifact store to temp file
            fileio.copy(filepath, temp_file)
            matrix = xgb.DMatrix(temp_file)

        return matrix

    def handle_return(self, matrix: xgb.DMatrix) -> None:
        """Create binary data file for DMatrix.

        Args:
            matrix: An xgboost DMatrix dataset
        """
        super().handle_return(matrix)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Make temp artifact
        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
            matrix.save_binary(temp_file.name)

            # copy file
            fileio.copy(temp_file.name, filepath)
