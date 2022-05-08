# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Kubeflow Pipelines based implementation of TFX components.
These components are lightweight wrappers around the KFP DSL's ContainerOp,
and ensure that the container gets called with the right set of input
arguments. It also ensures that each component exports named output
attributes that are consistent with those provided by the native TFX
components, thus ensuring that both types of pipeline definitions are
compatible.
Note: This requires Kubeflow Pipelines SDK to be installed.
"""

from typing import Dict, List, Set
import json
import re

from absl import logging
from coalescenceml.enums import StackComponentFlavor
from kfp import dsl
from kubernetes import client as k8s_client
from tfx.dsl.components.base import base_node as tfx_base_node
from tfx.dsl.components.base import base_component as tfx_base_component
from tfx.orchestration import data_types
from tfx.orchestration import pipeline as tfx_pipeline
# from tfx.orchestration.kubeflow.proto import kubeflow_pb2
from tfx.proto.orchestration import pipeline_pb2

from google.protobuf import json_format

from coalescenceml.artifact_store import LocalArtifactStore
from coalescenceml.metadata_store import SQLiteMetadataStore
from coalescenceml.constants import ENV_COML_PREVENT_PIPELINE_EXECUTION
from coalescenceml.directory import Directory
from coalescenceml.logger import get_logger
from coalescenceml.io.utils import get_global_config_directory
from coalescenceml.utils import source_utils

logger = get_logger(__name__)

# TODO(b/166202742): Consolidate container entrypoint with TFX image's default.
_COMMAND = ['python', '-m',
            'coalescenceml.integrations.kubeflow.orchestrator.container_entrypoint']

_WORKFLOW_ID_KEY = 'WORKFLOW_ID'


def _encode_runtime_parameter(param: data_types.RuntimeParameter) -> str:
    """Encode a runtime parameter into a placeholder for value substitution."""
    if param.ptype is int:
        type_enum = pipeline_pb2.RuntimeParameter.INT
    elif param.ptype is float:
        type_enum = pipeline_pb2.RuntimeParameter.DOUBLE
    else:
        type_enum = pipeline_pb2.RuntimeParameter.STRING
    type_str = pipeline_pb2.RuntimeParameter.Type.Name(type_enum)
    return f'{param.name}={type_str}:{str(dsl.PipelineParam(name=param.name))}'


def _replace_placeholder(component: tfx_base_node.BaseNode) -> None:
    """Replaces the RuntimeParameter placeholders with kfp.dsl.PipelineParam."""
    keys = list(component.exec_properties.keys())
    for key in keys:
        exec_property = component.exec_properties[key]
        if not isinstance(exec_property, data_types.RuntimeParameter):
            continue
        component.exec_properties[key] = str(
            dsl.PipelineParam(name=exec_property.name))


def get_input_artifact_type_mapping(
    step_component: tfx_base_component.BaseComponent,
) -> Dict[str, str]:
    """Returns a mapping from input artifact name to artifact type."""
    return {
        input_name: source_utils.resolve_class(channel.type)
        for input_name, channel in step_component.spec.inputs.items()
    }


class KubeComponent:
    """Kube component for all Kubeflow pipelines TFX components.
    Returns a wrapper around a KFP DSL ContainerOp class, and adds named output
    attributes that match the output names for the corresponding native TFX
    components.
    """

    def __init__(self,
                 component: tfx_base_component.BaseComponent,
                 depends_on: Set[dsl.ContainerOp],
                 image: str,
                 #  pipeline: tfx_pipeline.Pipeline,
                 #  pipeline_root: dsl.PipelineParam,
                 #  tfx_image: str,
                 #  kubeflow_metadata_config: kubeflow_pb2.KubeflowMetadataConfig,
                 tfx_ir: pipeline_pb2.Pipeline,
                 pod_labels_to_attach: Dict[str, str],
                 main_module: str,
                 step_module: str,
                 step_function_name: str,
                 runtime_parameters: List[data_types.RuntimeParameter],
                 metadata_ui_path: str = '/mlpipeline-ui-metadata.json'):
        """Creates a new Kubeflow-based component.
        This class essentially wraps a dsl.ContainerOp construct in Kubeflow
        Pipelines.
        Args:
          component: The logical TFX component to wrap.
          depends_on: The set of upstream KFP ContainerOp components that this
            component will depend on.
          pipeline: The logical TFX pipeline to which this component belongs.
          pipeline_root: The pipeline root specified, as a dsl.PipelineParam
          tfx_image: The container image to use for this component.
          kubeflow_metadata_config: Configuration settings for connecting to the
            MLMD store in a Kubeflow cluster.
          tfx_ir: The TFX intermedia representation of the pipeline.
          pod_labels_to_attach: Dict of pod labels to attach to the GKE pod.
          runtime_parameters: Runtime parameters of the pipeline.
          metadata_ui_path: File location for metadata-ui-metadata.json file.
        """

        vol_mount_dir = metadata_ui_path.split("/")[0]
        volumes: Dict[str, k8s_client.V1Volume] = {
            f"/{vol_mount_dir}": k8s_client.V1Volume(
                name=f"{vol_mount_dir}", empty_dir=k8s_client.V1EmptyDirVolumeSource()
            ),
        }

        _replace_placeholder(component)

        input_artifact_type_mapping = get_input_artifact_type_mapping(
            component
        )

        arguments = [
            # '--pipeline_root',
            # pipeline_root,
            # '--kubeflow_metadata_config',
            # json_format.MessageToJson(
            # message=kubeflow_metadata_config, preserving_proto_field_name=True),
            '--node_id',
            component.id,
            '--tfx_ir',
            json_format.MessageToJson(tfx_ir),
            '--metadata_ui_path',
            metadata_ui_path,
            "--main_module",
            main_module,
            "--step_module",
            step_module,
            "--step_name",
            step_function_name,
            "--input_artifact_type_mapping",
            json.dumps(input_artifact_type_mapping),
        ]

        for param in runtime_parameters:
            arguments.append('--runtime_parameter')
            arguments.append(_encode_runtime_parameter(param))

        stack = Directory().active_stack
        global_cfg_dir = get_global_config_directory()
        # TODO: Mount items which have local paths such as the local
        # artifact store and local metadata store.
        for stack_comp in stack.components.values():
            if (
                not isinstance(stack_comp, LocalArtifactStore) and 
                not isinstance(stack_comp, SQLiteMetadataStore)
            ):
                continue

            if stack_comp.TYPE == StackComponentFlavor.ARTIFACT_STORE:
                local_path = stack_comp.path
            else:
                local_path = stack_comp.uri

            host_path = k8s_client.V1HostPathVolumeSource(
                path=local_path, type="Directory"
            )
            volume_name = f"{stack_comp.TYPE.value}-{stack_comp.name}"
            volumes[local_path] = k8s_client.V1Volume(
                name=re.sub(r"[^0-9a-zA-Z-]+", "-", volume_name)
                .strip("-")
                .lower(),
                host_path=host_path,
            )
            logger.debug(
                "Adding host path volume for %s %s (path: %s) "
                "in kubeflow pipelines container.",
                stack_comp.TYPE.value,
                stack_comp.name,
                local_path,
            )

        self.container_op = dsl.ContainerOp(
            name=component.id,
            command=_COMMAND,
            image=image,
            arguments=arguments,
            output_artifact_paths={
                'mlpipeline-ui-metadata': metadata_ui_path,
            },
            pvolumes=volumes,
        )

        logging.info('Adding upstream dependencies for component %s',
                     self.container_op.name)
        for op in depends_on:
            logging.info('   ->  Component: %s', op.name)
            self.container_op.after(op)

        self.container_op.container.add_env_variable(
            k8s_client.V1EnvVar(
                name=ENV_COML_PREVENT_PIPELINE_EXECUTION, value="True"
            )
        )

        # if _WORKFLOW_ID_KEY in pipeline.additional_pipeline_args:
        #     # Allow overriding pipeline's run_id externally, primarily for testing.
        #     self.container_op.container.add_env_variable(
        #         k8s_client.V1EnvVar(
        #             name=_WORKFLOW_ID_KEY,
        #             value=pipeline.additional_pipeline_args[_WORKFLOW_ID_KEY]))
        # else:
        #     # Add the Argo workflow ID to the container's environment variable so it
        #     # can be used to uniquely place pipeline outputs under the pipeline_root.
        #     field_path = "metadata.labels['workflows.argoproj.io/workflow']"
        #     self.container_op.container.add_env_variable(
        #         k8s_client.V1EnvVar(
        #             name=_WORKFLOW_ID_KEY,
        #             value_from=k8s_client.V1EnvVarSource(
        #                 field_ref=k8s_client.V1ObjectFieldSelector(
        #                     field_path=field_path))))

        if pod_labels_to_attach:
            for k, v in pod_labels_to_attach.items():
                self.container_op.add_pod_label(k, v)
