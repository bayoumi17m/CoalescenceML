from __future__ import annotations

import os
from typing import Any, Type
import pandas


from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class
from coalescenceml.utils.json_utils import read_json, write_json


DATA_FILENAME = "data.parquet"

@register_producer_class
class pdDataFrameProducer(BaseProducer):
    """Producer to read data to and from pandas Dataframes."""

    ARTIFACT_TYPES = (DataArtifact,)
    TYPES = (pandas.core.frame.DataFrame,)

    def handle_input(self, data_type: Type[Any]) -> pandas.core.frame.DataFrame:
        """Reads dataframe from parquet file.
        Args:
            data_type: type of input to be processed
        Returns:
            Pandas dataframe with data
        """
        super().handle_input(data_type)

        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "rb"
        ) as fp:
            data = pandas.read_parquet(fp)

        return data

    def handle_return(self, df: pandas.core.frame.DataFrame) -> None:
        """Writes a np.ndarray to artifact store as parquet.
        Args:
            arr: Numpy arry to write.
        """
        super().handle_return(df)
        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "wb"
        ) as fp:
          df.to_parquet(fp)

