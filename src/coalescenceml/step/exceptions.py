from zenml.steps import BaseStepConfig

class StepContextError(Exception):
    """Raises exception when interacting with a StepContext
    in an unsupported way."""

class StepInterfaceError(Exception):
    """Raises exception when interacting with the Step interface
    in an unsupported way."""

class MissingStepParameterError(Exception):
    """Raises exceptions when a step parameter is missing when running a
    pipeline."""

    def __init__(
        self,
        step_name: str,
        missing_parameters: List[str],
        config_class: BaseStepConfig,
    ):
        """
        Initializes a MissingStepParameterError object.
        Args:
            step_name: Name of the step for which one or more parameters
                       are missing.
            missing_parameters: Names of all parameters which are missing.
            config_class: Class of the configuration object for which
                          the parameters are missing.
        """
        message = f"""
            Missing parameters {missing_parameters} for '{step_name}' step.
            There are two ways to solve this issue:
            (1) Specify a default value in the configuration class
            `{config_class.__name__}`
            (2) Specify the parameters in code when creating the pipeline:
            `my_pipeline({step_name}(config={config_class.__name__}(...))`
            """
        super().__init__(message)

