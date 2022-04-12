class PipelineInterfaceError(Exception):
    """Raises exception when interacting with the Pipeline interface
    in an unsupported way."""

class PipelineConfigurationError(Exception):
    """Raises exceptions when a pipeline configuration contains
    invalid values."""