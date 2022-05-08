from typing import ClassVar, Union

from ml_metadata.proto import metadata_store_pb2
from tfx.orchestration import metadata

from coalescenceml.metadata_store import BaseMetadataStore


class MySQLMetadataStore(BaseMetadataStore):
    """MySQL backend for CoalescenceML metadata store."""

    host: str
    port: int
    database: str
    username: str
    password: str

    # Class Configuration
    FLAVOR: ClassVar[str] = "mysql"

    def get_tfx_metadata_config(
        self,
    ) -> Union[
        metadata_store_pb2.ConnectionConfig,
        metadata_store_pb2.MetadataStoreClientConfig,
    ]:
        """Return tfx metadata config for mysql metadata store."""
        return metadata.mysql_metadata_connection_config(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )
