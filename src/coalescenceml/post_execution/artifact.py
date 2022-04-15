from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Type

from coalescenceml.logger import get_logger
from coalescenceml.utils import source_utils


if TYPE_CHECKING:
    from coalescenceml.metadata_store import BaseMetadataStore
    from coalescenceml.post_execution.step import StepView
    from coalescenceml.producers.base_producer import BaseProducer

logger = get_logger(__name__)


class ArtifactView:
    """Post-execution artifact class which can be used to read
    artifact data that was created during a pipeline execution.
    """

    def __init__(
        self,
        id_: int,
        type_: str,
        uri: str,
        producer: str,
        data_type: str,
        metadata_store: BaseMetadataStore,
        parent_step_id: int,
    ):
        """Initializes a post-execution artifact object.
        In most cases `ArtifactView` objects should not be created manually but
        retrieved from a `StepView` via the `inputs` or `outputs` properties.
        Args:
            id_: The artifact id.
            type_: The type of this artifact.
            uri: Specifies where the artifact data is stored.
            producer: Information needed to restore the producer
                that was used to write this artifact.
            data_type: The type of data that was passed to the producer
                when writing that artifact. Will be used as a default type
                to read the artifact.
            metadata_store: The metadata store which should be used to fetch
                additional information related to this pipeline.
            parent_step_id: The ID of the parent step.
        """
        self._id = id_
        self._type = type_
        self._uri = uri
        self._producer = producer
        self._data_type = data_type
        self._metadata_store = metadata_store
        self._parent_step_id = parent_step_id

    @property
    def id(self) -> int:
        """Returns the artifact id."""
        return self._id

    @property
    def type(self) -> str:
        """Returns the artifact type."""
        return self._type

    @property
    def data_type(self) -> str:
        """Returns the data type of the artifact."""
        return self._data_type

    @property
    def uri(self) -> str:
        """Returns the URI where the artifact data is stored."""
        return self._uri

    @property
    def parent_step_id(self) -> int:
        """Returns the ID of the parent step. This need not be equivalent to
        the ID of the producer step."""
        return self._parent_step_id

    @property
    def producer_step(self) -> StepView:
        """Returns the original StepView that produced the artifact."""
        # TODO: Replace with artifact.id instead of passing self if
        #  required.
        return self._metadata_store.get_producer_step_from_artifact(self)

    @property
    def is_cached(self) -> bool:
        """Returns True if artifact was cached in a previous run, else False."""
        # self._metadata_store.
        return self.producer_step.id != self.parent_step_id

    def read(
        self,
        output_data_type: Optional[Type[Any]] = None,
        producer_class: Optional[Type[BaseProducer]] = None,
    ) -> Any:
        """Produces the data stored in this artifact.
        Args:
            output_data_type: The datatype to which the producer should
                read, will be passed to the producers `handle_input` method.
            producer_class: The class of the producer that should be
                used to read the artifact data. If no producer class is
                given, we use the producer that was used to write the
                artifact during execution of the pipeline.
        Returns:
              The produced data.
        """

        if not producer_class:
            try:
                producer_class = source_utils.load_source_path_class(
                    self._producer
                )
            except (ModuleNotFoundError, AttributeError) as e:
                logger.error(
                    f"CoalescenceML can not locate and import the producer module "
                    f"{self._producer} which was used to write this "
                    f"artifact. If you want to read from it, please provide "
                    f"a 'producer_class'."
                )
                raise ModuleNotFoundError(e) from e

        if not output_data_type:
            try:
                output_data_type = source_utils.load_source_path_class(
                    self._data_type
                )
            except (ModuleNotFoundError, AttributeError) as e:
                logger.error(
                    f"CoalescenceML can not locate and import the data type of this "
                    f"artifact {self._data_type}. If you want to read "
                    f"from it, please provide a 'output_data_type'."
                )
                raise ModuleNotFoundError(e) from e

        logger.debug(
            "Using '%s' to read '%s' (uri: %s).",
            producer_class.__qualname__,
            self._type,
            self._uri,
        )

        # TODO: passing in `self` to initialize the producer only
        #  works because producers only require a `.uri` property at the
        #  moment.
        producer = producer_class(self)
        return producer.handle_input(output_data_type)

    def __repr__(self) -> str:
        """Returns a string representation of this artifact."""
        return (
            f"{self.__class__.__qualname__}(id={self._id}, "
            f"type='{self._type}', uri='{self._uri}', "
            f"producer='{self._producer}')"
        )

    def __eq__(self, other: Any) -> bool:
        """Returns whether the other object is referring to the
        same artifact."""
        if isinstance(other, ArtifactView):
            return self._id == other._id and self._uri == other._uri
        return False
