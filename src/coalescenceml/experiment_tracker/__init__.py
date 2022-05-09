"""
## Experiment Trackers

Experiment trackers help you track/log ML experiments by logging the
parameters, metrics, and model artifacts which allows you to compare
between runs. In general a single pipeline run is an experiment however,
one can run a series of experiments within a single pipeline.
"""

from coalescenceml.experiment_tracker.base_experiment_tracker import (
    BaseExperimentTracker,
)

__all__ = [
    "BaseExperimentTracker",
]
