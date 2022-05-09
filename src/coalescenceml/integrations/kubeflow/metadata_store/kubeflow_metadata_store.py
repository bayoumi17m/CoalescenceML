

from coalescenceml.directory import Directory
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.metadata_store import BaseMetadataStore
from coalescenceml.stack import Stack

logger = get_logger(__name__)

DEFAULT_KFP_METADATA_GRPC_PORT = 8081


def am_i_inside_kubeflow():
    pass


class KubeflowMetadataStore(BaseMetadataStore):
    pass
