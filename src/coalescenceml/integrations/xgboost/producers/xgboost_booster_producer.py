import os
import tempfile
from typing import Any, Type

import xgboost as xgb

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.producers import BaseProducer


DEFAULT_FILENAME = "model.xgb.json"


class XgboostBoosterProducer(BaseProducer):
    """Producer to read and write from xgboost.Booster."""

    TYPES = (xgb.Booster,)
    ARTIFACT_TYPES = (ModelArtifact,)

    def handle_input(self, data_type: Type[Any]) -> xgb.Booster:
        """Read XGB Booster model from JSON file.

        Args:
            data_type: Data type to be processed.

        Returns:
            XGB Booster model
        """
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Create temp file
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as tempfile:
            # Copy from artifact store to temp file
            fileio.copy(filepath, tempfile.name)
            booster = xgb.Booster()
            booster.load_model(tempfile.name)

        return booster

    def handle_return(self, booster: xgb.Booster) -> None:
        """Create JSON file for Booster.

        Args:
            booster: An xgboost Booster model
        """
        super().handle_return(booster)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Make temp artifact
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as tempfile:
            booster.save_model(tempfile.name)

            # copy file
            fileio.copy(tempfile.name, filepath)
