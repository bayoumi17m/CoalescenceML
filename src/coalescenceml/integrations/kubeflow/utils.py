from __future__ import annotations

import json
import logging
import time
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    cast,
)
import kfp
from kfp import dsl
import kfp.components as kfp_components

from tfx.orchestration.kubeflow import kubeflow_dag_runner
from tfx.dsl.compiler.compiler import Compiler
from tfx.dsl.compiler.constants import PIPELINE_RUN_ID_PARAMETER_NAME
from tfx.dsl.components.base import base_component
from tfx.orchestration import metadata
from tfx.orchestration.local import runner_utils
from tfx.orchestration.pipeline import Pipeline as TfxPipeline
from tfx.orchestration import pipeline as tfx_kf_pipeline
from tfx.orchestration.portable import launcher, runtime_parameter_utils
from tfx.proto.orchestration import executable_spec_pb2
from tfx.proto.orchestration.pipeline_pb2 import (
    Pipeline as Pb2Pipeline,
    PipelineNode,
)
from coalescenceml.directory import Directory
from coalescenceml.logger import get_logger
from coalescenceml.step import BaseStep
from coalescenceml.step.utils import (
    INTERNAL_EXECUTION_PARAMETER_PREFIX,
    PARAM_PIPELINE_PARAMETER_NAME,
)
from coalescenceml.utils import readability_utils
from coalescenceml.orchestrator import BaseOrchestrator, utils


if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack

logger = get_logger(__name__)


def create_kfp_pipeline(coalescenceml_pipeline: BasePipeline, stack: Stack, runtime_configuration: RuntimeConfiguration
                        ) -> function:
    """Creates a kfp pipeline from a CoalescenceML pipeline."""
    tfx_pipeline: TfxPipeline = utils.create_tfx_pipeline(
        coalescenceml_pipeline, stack=stack
    )
    pipeline_root = tfx_pipeline.pipeline_info.pipeline_root
    if not isinstance(pipeline_root, str):
        raise TypeError(
            "TFX Pipeline root may not be a Placeholder, "
            "but must be a specific string."
        )
    dsl_pipeline_root = dsl.PipelineParam(
        name=tfx_kf_pipeline.ROOT_PARAMETER.name,
        value=tfx_pipeline.pipeline_info.pipeline_root)

    kfp_runner = kubeflow_dag_runner.KubeflowDagRunner(
        output_dir=runtime_configuration.get("kf_output_dir"),
        output_filename=runtime_configuration.get("kf_output_filename"),
        config=runtime_configuration.get("kf_dag_runner_config"),
        pods_label_to_attach=runtime_configuration.get(
            "kf_pods_label_to_attach")
    )

    # artifact_store = stack.artifact_store
    # metadata_store = stack.metadata_store

    return kfp_runner._construct_pipeline_graph(tfx_pipeline, dsl_pipeline_root)
