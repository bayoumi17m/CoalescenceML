"""
## Artifacts

Artifacts are the data that power your experimentation and model training.
Steps are what actuall produce artifacts which are then placed in the
artifact store.
"""
from coalescenceml.artifacts.data_analysis_artifact import DataAnalysisArtifact
from coalescenceml.artifacts.data_artifact import DataArtifact
from coalescenceml.artifacts.metrics_artifact import MetricsArtifact
from coalescenceml.artifacts.model_artifact import ModelArtifact


__all__ = [
    "DataAnalysisArtifact",
    "DataArtifact",
    "ModelArtifact",
    "MetricsArtifact",
]
