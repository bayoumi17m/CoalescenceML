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
from coalescenceml.integrations.kubeflow.orchestrator.kubeflow_dag_runner import KubeflowDagRunner
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

    # Build dag from all steps in the pipeline and compile it into a yaml spec
    runner = KubeflowDagRunner(output_dir=runtime_configuration.get(
        'output_dir'), output_filename=runtime_configuration.get('output_filename'))  # TODO

    return runner.run(tfx_pipeline)
