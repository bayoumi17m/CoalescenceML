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
import os
from coalescenceml.integrations.kubeflow.orchestrator.kubeflow_dag_runner import KubeflowDagRunner
from integrations.kubeflow.orchestrator.kubeflow_dag_runner import KubeflowDagRunnerConfig
from tfx.orchestration.pipeline import Pipeline as TfxPipeline
from coalescenceml.logger import get_logger
from coalescenceml.orchestrator import utils


if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )
    from coalescenceml.stack import Stack

logger = get_logger(__name__)


def create_kfp_pipeline(coalescenceml_pipeline: BasePipeline, stack: Stack, runtime_configuration: RuntimeConfiguration, image_name: str) -> str:
    """Creates a kfp pipeline from a CoalescenceML pipeline."""

    tfx_pipeline: TfxPipeline = utils.create_tfx_pipeline(
        coalescenceml_pipeline, stack=stack
    )
    kube_config = KubeflowDagRunnerConfig(tfx_image=image_name)
    # Build dag from all steps in the pipeline and compile it into a yaml spec
    runner = KubeflowDagRunner(output_dir=runtime_configuration.get(
        'output_dir'), output_filename=runtime_configuration.get('output_filename'), config=kube_config)
    # returns file name
    runner.run(tfx_pipeline)
    if runtime_configuration.get('output_dir'):
        return os.path.join(runtime_configuration.get('output_dir'),
                            runtime_configuration.get('output_filename'))
    else:
        return runtime_configuration.get('output_filename')
