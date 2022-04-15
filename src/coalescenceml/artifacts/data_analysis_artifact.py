from coalescenceml.artifacts.base_artifact import BaseArtifact


class DataAnalysisArtifact(BaseArtifact):
    """Class for Coalescence Data Analysis artifacts.

    This is a base setup for all things created by things such as
    data profiling, data drift, model drift, etc.
    """

    TYPE_NAME = "DataAnalysisArtifact"
