from __future__ import annotations

import os
from typing import Any, Type
import pandas

from coalescenceml.artifacts import DataArtifact
from coalescenceml.io import fileio
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.producers.producer_registry import register_producer_class


DATA_FILENAME = "data.pkl"

@register_producer_class
class pdSeriesProducer(BaseProducer):
    """Producer to read data to and from pandas Series."""

    ARTIFACT_TYPES = (DataArtifact,)
    TYPES = (pandas.core.frame.DataFrame,)

    def handle_input(self, data_type: Type[Any]) -> pandas.core.series.Series:
        """Reads dataframe from pkl file.
        Args:
            data_type: type of input to be processed
        Returns:
            Pandas series with data
        """
        super().handle_input(data_type)

        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "rb"
        ) as fp:
            data = pandas.read_pickle(fp)

        return data

    def handle_return(self, series: pandas.core.series.Series) -> None:
        """Writes a pd.Series to artifact store as pickle.
        Args:
            series: pandas Series to write.
        """
        super().handle_return(series)
        with fileio.open(
            os.path.join(self.artifact.uri, DATA_FILENAME), "wb"
        ) as fp:
          series.to_pickle(fp)