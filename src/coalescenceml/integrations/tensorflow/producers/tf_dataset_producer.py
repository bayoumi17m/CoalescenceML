import os
from typing import Any, Type

import tensorflow as tf

from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


logger = get_logger(__name__)
DEFAULT_FILENAME = "tf_saved_data"


@register_producer_class
class TensorflowDatasetProducer(BaseProducer):
    """Read/Write TF Dataset files."""

    ARTIFACT_TYPES = (DataArtifact,)
    TYPES = (tf.data.Dataset,)

    def handle_input(self, data_type: Type[Any]) -> tf.data.Dataset:
        """Read tf.data.Dataset data into memory."""
        super().handle_input(data_type)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        contents = tf.data.experimental.load(filepath)
        return contents

    def handle_return(self, dataset: tf.data.Dataset) -> None:
        """Writes a tf.data.Dataset object to filesystem."""
        super().handle_return(model)
        filepath = os.path.join(self.artifact.uri, DEFAULT_FILENAME)
        tf.data.experimental.save(
            dataset, filepath, compression=None, shard_func=None,
        )
