from coalescenceml.enums import StackComponentFlavor
from coalescenceml.metadata_store import MySQLMetadataStore


def test_mysql_metadata_store_attributes():
    """Tests that the basic attributes of the mysql metadata store are set
    correctly."""
    metadata_store = MySQLMetadataStore(
        name="", host="", port=0, database="", username="", password=""
    )
    assert metadata_store.TYPE == StackComponentFlavor.METADATA_STORE
    assert metadata_store.FLAVOR == "mysql"