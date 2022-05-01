import os
from typing import Any, Type

import tensorflow as tf

from coalescenceml.artifacts import ModelArtifact
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "model.keras.hdf5"


@register_producer_class
class KerasModelProducer(BaseProducer):
    """Read/Write Keras Model files."""

    ARTIFACT_TYPES = (ModelArtifact,)
    TYPES = (tf.keras.Model,)

    def handle_input(self, data_type: Type[Any]) -> tf.keras.Model:
        """Reads keras model from h5 file."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        contents = tf.keras.models.load_model(filepath)
        return contents

    def handle_return(self, model: tf.keras.Model) -> None:
        """Writes a keras model to artifact store as h5"""
        super().handle_return(model)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        model.save_model(filepath)
