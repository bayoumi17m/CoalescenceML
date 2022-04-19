from typing import ClassVar, Union

from ml_metadata.proto import metadata_store_pb2
from pydantic import validator
from tfx.orchestration import metadata

from coalescenceml.io import utils
from coalescenceml.metadata_store import BaseMetadataStore


class SQLiteMetadataStore(BaseMetadataStore):
    """SQLite backend for CoalescenceML metadata store."""

    uri: str

    # Class Configuration
    FLAVOR: ClassVar[str] = "sqlite"

    @property
    def upgrade_migration_enabled(self) -> bool:
        """Return True to enable automatic database schema migration."""
        return True

    def get_tfx_metadata_config(
        self,
    ) -> Union[
        metadata_store_pb2.ConnectionConfig,
        metadata_store_pb2.MetadataStoreClientConfig,
    ]:
        """Return tfx metadata config for sqlite metadata store."""
        return metadata.sqlite_metadata_connection_config(self.uri)

    @validator("uri")
    def ensure_uri_is_local(cls, uri: str) -> str:
        """Ensures that the metadata store uri is local."""
        if utils.is_remote(uri):
            raise ValueError(
                f"Uri '{uri}' specified for SQLiteMetadataStore is not a "
                f"local uri."
            )

        return uri
