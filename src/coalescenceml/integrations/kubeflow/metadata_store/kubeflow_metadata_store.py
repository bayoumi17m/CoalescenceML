import os
import subprocess
import sys
import time
from typing import TYPE_CHECKING, ClassVar, Optional, Tuple, Union, cast

from kubernetes import config as k8s_config
from ml_metadata.proto import metadata_store_pb2

from coalescenceml.directory import Directory
from coalescenceml.integrations.constants import KUBEFLOW
from coalescenceml.integrations.kubeflow.orchestrator import KubeflowOrchestrator
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.metadata_store import BaseMetadataStore
from coalescenceml.stack import Stack
from coalescenceml.stack import StackValidator
from coalescenceml.stack.stack_component_class_registry import (
    register_stack_component_class
)

logger = get_logger(__name__)

DEFAULT_KFP_METADATA_GRPC_PORT = 8081


def i_am_inside_kubeflow():
    """Return if current python process is inside a KFP pod."""
    if "KFP_POD_NAME" not in os.environ:
        return False
    
    try:
        k8s_config.load_incluster_config()
        return True
    except k8s_config.ConfigException:
        return False


@register_stack_component_class
class KubeflowMetadataStore(BaseMetadataStore):
    """Kubeflow GRPC backend for metadata store"""

    upgrade_migration_enabled: bool = False
    host: str = "127.0.0.1"
    port: int = DEFAULT_KFP_METADATA_GRPC_PORT

    # Class Configuration
    FLAVOR: ClassVar[str] = KUBEFLOW 

    @property
    def validator(self) -> Optional[StackValidator]:

        def ensure_kfp_orchestrator(stack: Stack) -> Tuple[bool, str]:
            return (
                stack.orchestrator.FLAVOR == KUBEFLOW,
                "The Kubeflow metadata store can only be used with a "
                "Kubeflow orchestrator."
            )
        return StackValidator(custom_validation_function=ensure_kfp_orchestrator)

    def get_tfx_metadata_config(self) -> Union[
        metadata_store_pb2.ConnectionConfig,
        metadata_store_pb2.MetadataStoreClientConfig
    ]:
        """"""
        connection_config = metadata_store_pb2.MetadataStoreClientConfig()
        if i_am_inside_kubeflow():
            connection_config.host = os.environ["METADATA_GRPC_SERVICE_HOST"]
            connection_config.port = int(os.environ["METADATA_GRPC_SERVICE_PORT"])
        else:
            if not self.is_running:
                raise RuntimeError(
                    ""
                )
            
            connection_config.host = self.host
            connection_config.port = self.port
        
        return connection_config

    @property
    def kfp_orchestrator(self) -> KubeflowOrchestrator:
        orch = Directory(skip_directory_check=True).activate_stack.orchestrator
        return cast(KubeflowOrchestrator, orch)
    
    @property
    def kubernetes_context(self) -> str:
        kubernetes_context = self.kfp_orchestrator.kubernetes_context
        return kubernetes_context
    
    @property
    def root_directory(self) -> str:
        """"""
        return os.path.join(
            self.kfp_orchestrator.root_directory,
            "metadata-store",
            str(self.uuid),
        )

    @property
    def pid_file_path(self)->str:
        return os.path.join(self.root_directory, "kubeflow_daemon.pid")
    
    @property
    def log_file_path(self) -> str:
        return os.path.join(self.root_directory, "kubeflow_daemon.log")
    
    @property
    def is_provisioned(self) -> bool:
        return fileio.exists(self.root_directory)
    
    @property
    def is_running(self) -> bool:
        from coalescenceml.utils.daemon import check_if_running
        if not check_if_running(self.pid_file_path):
            return False
        else:
            return True
    
    def provision(self) -> None:
        fileio.makedirs(self.root_directory)
    
    def deprovision(self) -> None:
        if fileio.exists(self.root_directory):
            fileio.remove(self.root_directory)
    
    def resume(self) -> None:
        if self.is_running:
            return
        
        self.start_kfp_metadata_daemon()
        self.wait_until_ready()
    
    def suspend(self) -> None:
        if not self.is_running:
            return
        
        self.stop_kfp_metadata_daemon()
    
    def start_kfp_metadata_daemon(self):
        command = [
            "kubectl",
            "--context",
            self.kubernetes_context,
            "--namespace",
            "kubeflow",
            "port-forward",
            "svc/metadata-grpc-service",
            f"{self.port}:8080",
        ]

        if sys.platform == "win32":
            logger.warning("NO WORK ON WINDOWS :(")
        else:
            from coalescenceml.utils import daemon

            def daemon_function() -> None:
                subprocess.check_call(command)
            
            daemon.run_daemon(
                daemon_function,
                pid_file=self.pid_file_path,
                log_file=self.log_file_path,
            )
            logger.info(
                "Daemon process running. Check it out!"
            )

    def stop_kfp_metadata_daemon(self):
        from coalescenceml.utils import daemon

        daemon.stop_daemon(self.pid_file_path)
        fileio.remove(self.pid_file_path)

    def wait_until_ready(self, timeout: int=60):
        while True:
            try:
                self.get_pipelines()
                break
            except Exception as e:
                if timeout <= 0:
                    raise RuntimeError(
                        str(e)
                    )
                
                timeout -= 10
                time.sleep(10)

