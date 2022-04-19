import pydantic
import pytest

from coalescenceml.enums import StackComponentFlavor
from coalescenceml.metadata_store import SQLiteMetadataStore


def test_sqlite_metadata_store_attributes():
    """Tests that the basic attributes of the sqlite metadata store are set
    correctly."""
    metadata_store = SQLiteMetadataStore(name="", uri="")
    assert metadata_store.TYPE == StackComponentFlavor.METADATA_STORE
    assert metadata_store.FLAVOR == "sqlite"


def test_sqlite_metadata_store_only_supports_local_uris():
    """Checks that a sqlite metadata store can only be initialized with a local
    uri."""
    with pytest.raises(pydantic.ValidationError):
        SQLiteMetadataStore(name="", uri="gs://remote/uri")

    with pytest.raises(pydantic.ValidationError):
        SQLiteMetadataStore(name="", uri="s3://remote/uri")

    metadata_store = SQLiteMetadataStore(name="", uri="/local/uri")
    assert metadata_store.uri == "/local/uri"