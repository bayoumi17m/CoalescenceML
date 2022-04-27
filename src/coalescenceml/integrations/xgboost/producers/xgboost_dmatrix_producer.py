import os
import tempfile
from typing import Any, Type

import xgboost as xgb

from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers import BaseProducer


DEFAULT_FILENAME = "data.xgb.binary"


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

        # Create temp file
        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as tempfile:
            # Copy from artifact store to temp file
            fileio.copy(filepath, tempfile.name)
            matrix = xgb.DMatrix(tempfile.name)

        return matrix

    def handle_return(self, matrix: xgb.DMatrix) -> None:
        """Create binary data file for DMatrix.

        Args:
            matrix: An xgboost DMatrix dataset
        """
        super().handle_return(matrix)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Make temp artifact
        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as tempfile:
            matrix.save_binary(tempfile.name)

            # copy file
            fileio.copy(tempfile.name, filepath)
