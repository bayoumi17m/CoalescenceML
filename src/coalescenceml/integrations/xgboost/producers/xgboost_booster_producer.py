import os
import tempfile
from typing import Any, Type

import xgboost as xgb

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.producers import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


DEFAULT_FILENAME = "model.xgb.json"


@register_producer_class
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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, DEFAULT_FILENAME)
            # Create temp file
            # Copy from artifact store to temp file
            fileio.copy(filepath, temp_file)
            booster = xgb.Booster()
            booster.load_model(temp_file)

        return booster

    def handle_return(self, booster: xgb.Booster) -> None:
        """Create JSON file for Booster.

        Args:
            booster: An xgboost Booster model
        """
        super().handle_return(booster)

        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)

        # Make temp artifact
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=True) as temp_file:
            booster.save_model(temp_file.name)

            # copy file
            fileio.copy(temp_file.name, filepath)
