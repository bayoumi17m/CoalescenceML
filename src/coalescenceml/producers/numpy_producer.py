from __future__ import annotations
import os
from typing import Any, Type

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from numpy.typing import NDArray

from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import producer_registry
from coalescenceml.utils.json_utils import read_json, write_json


DATA_FILENAME = "data.parquet"
SHAPE_FILENAME = "shape.json"
DATA_VAR = "data_var"


class NumpyProducer(BaseProducer):
    """Producer to read data to and from numpy arrays."""

    TYPES = (np.ndarray,)
    ARTIFACT_TYPES = (DataArtifact,)

    def handle_input(self, data_type: Type[Any]) -> NDArray[Any]:
        """Reads numpy array from parquet file.

        Args:
            data_type: type of input to be processed

        Returns:
            Numpy array with data
        """
        super().handle_input(data_type)

        shape_dict = read_json(
            os.path.join(self.artifact.uri, SHAPE_FILENAME)
        )
        shape_tuple = tuple(shape_dict.value())

        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "rb"
        ) as fp:
            input_stream = pa.input_stream(fp)
            data = pq.read_table(input_stream)

        vals = getattr(data.to_pandas(), DATA_VAR).values
        return np.reshape(vals, shape_tuple)


    def handle_return(self, arr: NDarray[Any]) -> None:
        """Writes a np.ndarray to artifact store as parquet.

        Args:
            arr: Numpy arry to write.
        """
        super().handle_return(arr)

        write_json(
            os.path.join(self.artifact.uri, SHAPE_FILENAME),
            {str(i): d for i, d in enumerate(arr.shape)}
        )

        pa_table = pa.table({DATA_VAR: arr.flatten()})
        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "wb"
        ) as fp:
            stream = pa.output_stream(f)
            pq.write_table(pa_table, stream)


producer_registry.register_producer(np.ndarray, NumpyProducer)