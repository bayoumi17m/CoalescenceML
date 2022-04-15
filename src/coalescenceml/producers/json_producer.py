import os
from typing import Any, Type

from coalescenceml.artifacts import DataAnalysisArtifact, DataArtifact
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class
from coalescenceml.utils import json_utils


logger = get_logger(__name__)
DEFAULT_FILENAME = "data.json"


@register_producer_class
class JSONProducer(BaseProducer):
    """Read/Write JSON files."""

    # since these are the 'correct' way to annotate these types.

    ARTIFACT_TYPES = (
        DataArtifact,
        DataAnalysisArtifact,
    )
    TYPES = (
        int,
        str,
        bytes,
        dict,
        float,
        list,
        tuple,
        bool,
    )

    def handle_input(self, data_type: Type[Any]) -> Any:
        """Reads basic primitive types from json."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        contents = json_utils.read_json(filepath)
        if type(contents) != data_type:
            # TODO: Raise error or try to coerce
            logger.debug(
                f"Contents {contents} was type {type(contents)} but expected "
                f"{data_type}"
            )
        return contents

    def handle_return(self, data: Any) -> None:
        """Handles basic built-in types and stores them as json"""
        super().handle_return(data)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        json_utils.write_json(filepath, data)
