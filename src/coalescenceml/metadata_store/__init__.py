"""
## Metadata Store
The configuration of each pipeline, step, backend, and produced artifacts are
all tracked within the metadata store. The metadata store is an SQL database,
and can be `sqlite` or `mysql`.

Metadata are the pieces of information tracked about the pipelines, experiments
and configurations that you are running with CoML. 
"""

from coalescenceml.metadata_store.base_metadata_store import BaseMetadataStore
from coalescenceml.metadata_store.mysql_metadata_store import MySQLMetadataStore
from coalescenceml.metadata_store.sqlite_metadata_store import (
    SQLiteMetadataStore,
)


__all__ = [
    "BaseMetadataStore",
    "MySQLMetadataStore",
    "SQLiteMetadataStore",
]
