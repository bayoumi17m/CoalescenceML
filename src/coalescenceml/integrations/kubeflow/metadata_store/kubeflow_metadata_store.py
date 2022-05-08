import os

from kubernetes import config as k8s_config

from coalescenceml.directory import Directory
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.metadata_store import BaseMetadataStore
from coalescenceml.stack import Stack

logger = get_logger(__name__)

DEFAULT_KFP_METADATA_GRPC_PORT = 8081


def am_i_inside_kubeflow():
    """Return if current python process is inside a KFP pod."""
    if "KFP_POD_NAME" not in os.environ:
        return False
    
    try:
        k8s_config.load_incluster_config()
        return True
    except k8s_config.ConfigException:
        return False


class KubeflowMetadataStore(BaseMetadataStore):
    pass
