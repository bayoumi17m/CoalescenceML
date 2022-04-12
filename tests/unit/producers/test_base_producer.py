import pytest

from coalescenceml.artifacts import DataArtifact
from coalescenceml.producers.exceptions import ProducerInterfaceError
from coalescenceml.producers.base_producer import BaseProducer


class TestProducer(BaseProducer):
    __test__ = False
    ASSOCIATED_TYPES = (int,)


def test_producer_raises_an_exception_if_associated_types_are_no_classes():
    """Tests that a producer can only define classes as associated types."""
    with pytest.raises(ProducerInterfaceError):

        class InvalidProducer(BaseProducer):
            ASSOCIATED_TYPES = ("not_a_class",)


def test_producer_raises_an_exception_if_associated_artifact_types_are_no_artifacts():
    """Tests that a producer can only define `BaseArtifact` subclasses as
    associated artifact types."""
    with pytest.raises(ProducerInterfaceError):

        class InvalidProducer(BaseProducer):
            ASSOCIATED_TYPES = (int,)
            ASSOCIATED_ARTIFACT_TYPES = (DataArtifact, int, "not_a_class")


def test_producer_raises_an_exception_when_asked_to_read_unfamiliar_type():
    """Tests that a producer fails if it's asked to read the artifact to a
    non-associated type."""
    producer = TestProducer(artifact=DataArtifact())

    with pytest.raises(TypeError):
        producer.handle_input(data_type=str)


def test_producer_raises_an_exception_when_asked_to_write_unfamiliar_type():
    """Tests that a producer fails if it's asked to write data of a
    non-associated type."""
    producer = TestProducer(artifact=DataArtifact())

    with pytest.raises(TypeError):
        producer.handle_return(data="some_string")