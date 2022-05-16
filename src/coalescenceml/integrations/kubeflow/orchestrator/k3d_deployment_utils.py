import json
import shutil
import subprocess
import sys
import time

import coalescenceml.io.utils
from coalescenceml.io import fileio
from coalescenceml.logger import get_logger
from coalescenceml.utils import yaml_utils


KFP_VERSION = "release-1.8"
# Name of the K3S image to use for the local K3D cluster.
# The version (e.g. v1.23.5) refers to a specific kubernetes release and is
# fixed as KFP doesn't support the newest releases immediately.
K3S_IMAGE_NAME = "rancher/k3s:v1.23.5-k3s1"

logger = get_logger(__name__)


def check_prereqs(
    skip_k3d: bool = False
) -> bool:
    """"""
    k3d_installed = skip_k3d or shutil.which("k3d") is not None
    kubectl_installed = shutil.which("kubectl") is not None
    
    return k3d_installed and kubectl_installed


def write_local_registry_yaml(
    yaml_path: str, registry_name: str, registry_uri: str
) -> None:
    """"""
    yaml_content = {
        "mirrors": {registry_uri: {"endpoint": [f"http://{registry_name}"]}}
    }
    yaml_utils.write_yaml(yaml_path, yaml_content)


def k3d_cluster_exists(cluster_name: str) -> bool:
    """"""
    output = subprocess.check_output(
        ["k3d", "cluster", "list", "--output", "json"]
    )
    clusters = json.loads(output)
    for cluster in clusters:
        if cluster["name"] == cluster_name:
            return True
    return False


def k3d_cluster_running(cluster_name: str) -> bool:
    """"""
    output = subprocess.check_output(
        ["k3d", "cluster", "list", "--output", "json"]
    )
    clusters = json.loads(output)
    for cluster in clusters:
        if cluster["name"] == cluster_name:
            server_count: int = cluster["serversCount"]
            servers_running: int = cluster["serversRunning"]
            return servers_running == server_count
    return False


def create_k3d_cluster(
    cluster_name: str, registry_name: str, registry_config_path: str
) -> None:
    """"""
    logger.info("Creating local K3D cluster '%s'.", cluster_name)
    global_config_dir_path = coalescenceml.io.utils.get_global_config_directory()
    subprocess.check_call(
        [
            "k3d",
            "cluster",
            "create",
            cluster_name,
            "--image",
            K3S_IMAGE_NAME,
            "--registry-create",
            registry_name,
            "--registry-config",
            registry_config_path,
            "--volume",
            f"{global_config_dir_path}:{global_config_dir_path}",
            #"--agents",
            #"4",
        ]
    )
    logger.info("Finished K3D cluster creation.")


def start_k3d_cluster(cluster_name: str) -> None:
    """"""
    subprocess.check_call(["k3d", "cluster", "start", cluster_name])
    logger.info("Started local k3d cluster '%s'.", cluster_name)


def stop_k3d_cluster(cluster_name: str) -> None:
    """"""
    subprocess.check_call(["k3d", "cluster", "stop", cluster_name])
    logger.info("Stopped local k3d cluster '%s'.", cluster_name)


def delete_k3d_cluster(cluster_name: str) -> None:
    """"""
    subprocess.check_call(["k3d", "cluster", "delete", cluster_name])
    logger.info("Deleted local k3d cluster '%s'.", cluster_name)


def kubeflow_pipelines_ready(kubernetes_context: str) -> bool:
    """"""
    try:
        subprocess.check_call(
            [
                "kubectl",
                "--context",
                kubernetes_context,
                "--namespace",
                "kubeflow",
                "wait",
                "--for",
                "condition=ready",
                "--timeout=0s",
                "pods",
                "-l",
                "application-crd-id=kubeflow-pipelines",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def wait_until_kubeflow_pipelines_ready(kubernetes_context: str) -> None:
    """"""
    logger.info(
        "Waiting for all Kubeflow Pipelines pods to be ready (this might "
        "take a few minutes)."
    )
    while True:
        logger.info("Current pod status:")
        subprocess.check_call(
            [
                "kubectl",
                "--context",
                kubernetes_context,
                "--namespace",
                "kubeflow",
                "get",
                "pods",
            ]
        )
        if kubeflow_pipelines_ready(kubernetes_context=kubernetes_context):
            break

        logger.info("One or more pods not ready yet, waiting for 30 seconds...")
        time.sleep(30)
    

def deploy_kubeflow_pipelines(kubernetes_context: str) -> None:
    """"""
    logger.info("Deploying Kubeflow Pipelines.")
    subprocess.check_call(
        [
            "kubectl",
            "--context",
            kubernetes_context,
            "apply",
            "-k",
            f"github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref={KFP_VERSION}",
        ]
    )
    subprocess.check_call(
        [
            "kubectl",
            "--context",
            kubernetes_context,
            "wait",
            "--timeout=60s",
            "--for",
            "condition=established",
            "crd/applications.app.k8s.io",
        ]
    )
    subprocess.check_call(
        [
            "kubectl",
            "--context",
            kubernetes_context,
            "apply",
            "-k",
            f"github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic-pns?ref={KFP_VERSION}",
        ]
    )

    wait_until_kubeflow_pipelines_ready(kubernetes_context=kubernetes_context)
    logger.info("Finished Kubeflow Pipelines setup.")


def add_hostpath_to_kubeflow_pipelines(
    kubernetes_context: str, local_path: str
) -> None:
    """"""
    logger.info("Patching Kubeflow Pipelines to mount a local folder.")

    pod_template = {
        "spec": {
            "serviceAccountName": "kubeflow-pipelines-viewer",
            "containers": [
                {
                    "volumeMounts": [
                        {
                            "mountPath": local_path,
                            "name": "local-artifact-store",
                        }
                    ]
                }
            ],
            "volumes": [
                {
                    "hostPath": {
                        "path": local_path,
                        "type": "Directory",
                    },
                    "name": "local-artifact-store",
                }
            ],
        }
    }
    pod_template_json = json.dumps(pod_template, indent=2)
    config_map_data = {"data": {"viewer-pod-template.json": pod_template_json}}
    config_map_data_json = json.dumps(config_map_data, indent=2)

    logger.debug(
        "Adding host path volume for local path `%s` to kubeflow pipeline"
        "viewer pod template configuration.",
        local_path,
    )
    subprocess.check_call(
        [
            "kubectl",
            "--context",
            kubernetes_context,
            "-n",
            "kubeflow",
            "patch",
            "configmap/ml-pipeline-ui-configmap",
            "--type",
            "merge",
            "-p",
            config_map_data_json,
        ]
    )

    deployment_patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "ml-pipeline-ui",
                            "volumeMounts": [
                                {
                                    "mountPath": local_path,
                                    "name": "local-artifact-store",
                                }
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "hostPath": {
                                "path": local_path,
                                "type": "Directory",
                            },
                            "name": "local-artifact-store",
                        }
                    ],
                }
            }
        }
    }
    deployment_patch_json = json.dumps(deployment_patch, indent=2)

    logger.debug(
        "Adding host path volume for local path `%s` to the kubeflow UI",
        local_path,
    )
    subprocess.check_call(
        [
            "kubectl",
            "--context",
            kubernetes_context,
            "-n",
            "kubeflow",
            "patch",
            "deployment/ml-pipeline-ui",
            "--type",
            "strategic",
            "-p",
            deployment_patch_json,
        ]
    )
    wait_until_kubeflow_pipelines_ready(kubernetes_context=kubernetes_context)

    logger.info("Finished patching Kubeflow Pipelines setup.")


def start_kfp_ui_daemon(
    pid_file_path: str,
    log_file_path: str,
    port: int,
    kubernetes_context: str,
) -> None:
    """"""
    command = [
        "kubectl",
        "--context",
        kubernetes_context,
        "--namespace",
        "kubeflow",
        "port-forward",
        "svc/ml-pipeline-ui",
        f"{port}:80",
    ]
    if sys.platform == "win32":
        logger.warning(
            "Daemon functionality not supported on Windows. "
            "In order to access the Kubeflow Pipelines UI at "
            "http://localhost:%d/, please run '%s' in a separate command "
            "line shell.",
            port,
            " ".join(command),
        )
    else:
        from coalescenceml.utils import daemon

        def _daemon_function() -> None:
            """Port-forwards the Kubeflow Pipelines UI pod."""
            subprocess.check_call(command)

        daemon.run_daemon(
            _daemon_function, pid_file=pid_file_path, log_file=log_file_path
        )
        logger.info(
            "Started Kubeflow Pipelines UI daemon (check the daemon logs at %s "
            "in case you're not able to view the UI). The Kubeflow Pipelines "
            "UI should now be accessible at http://localhost:%d/.",
            log_file_path,
            port,
        )


def stop_kfp_ui_daemon(pid_file_path: str) -> None:
    """"""
    if fileio.exists(pid_file_path):
        from coalescenceml.utils import daemon

        daemon.stop_daemon(pid_file_path)
        fileio.remove(pid_file_path)
        logger.info("Stopped Kubeflow Pipelines UI daemon.")
