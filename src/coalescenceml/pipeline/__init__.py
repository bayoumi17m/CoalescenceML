"""
## Pipelines

A CoalescenceML pipeline is a sequence of tasks that execute in a specific order and
yield artifacts. The artifacts are stored within the artifact store and indexed
via the metadata store. Each individual task within a pipeline is known as a
step.
"""


from coalescenceml.pipeline.base_pipeline import BasePipeline
from coalescenceml.pipeline.pipeline_decorator import pipeline
from coalescenceml.pipeline.schedule import Schedule


__all__ = ["BasePipeline", "pipeline", "Schedule"]
