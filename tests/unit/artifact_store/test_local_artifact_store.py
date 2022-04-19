import pytest

from coalescenceml.artifact_store import LocalArtifactStore
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.artifact_store.exceptions import ArtifactStoreInterfaceError


def test_local_artifact_store_attributes():
    """Tests that the basic attributes of the local artifact store are set
    correctly."""
    artifact_store = LocalArtifactStore(name="", path="/tmp")
    assert artifact_store.TYPE == StackComponentFlavor.ARTIFACT_STORE
    assert artifact_store.FLAVOR == "local"


def test_local_artifact_store_only_supports_local_paths():
    """Checks that a local artifact store can only be initialized with a local
    path."""
    with pytest.raises(ArtifactStoreInterfaceError):
        LocalArtifactStore(name="", path="gs://remote/path")

    with pytest.raises(ArtifactStoreInterfaceError):
        LocalArtifactStore(name="", path="s3://remote/path")

    artifact_store = LocalArtifactStore(name="", path="/local/path")
    assert artifact_store.path == "/local/path"