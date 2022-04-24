from coalescenceml.environment import BaseEnvironmentComponent


STEP_ENVIRONMENT_NAME = "step_environment"


class StepEnvironment(BaseEnvironmentComponent):
    """Provides additional information about a step runtime inside a step
    function in the form of an Environment component.

    This class can be used from within a pipeline step implementation to
    access additional information about the runtime parameters of a pipeline
    step, such as the pipeline name, pipeline run ID and other pipeline runtime
    information. To use it, access it inside your step function like this:

    ```python
    from coalescenceml.environment import Environment

    @step
    def my_step(...)
        env = Environment().step_environment
        do_something_with(env.pipeline_name, env.pipeline_run_id, env.step_name)
    ```

    """

    NAME = STEP_ENVIRONMENT_NAME

    def __init__(
        self,
        pipeline_name: str,
        pipeline_run_id: str,
        step_name: str,
    ):
        """Initialize the environment of the currently running
        step.

        Args:
            pipeline_name: the name of the currently running pipeline
            pipeline_run_id: the ID of the currently running pipeline
            step_name: the name of the currently running step
        """
        super().__init__()
        self._pipeline_name = pipeline_name
        self._pipeline_run_id = pipeline_run_id
        self._step_name = step_name

    @property
    def pipeline_name(self) -> str:
        """The name of the currently running pipeline."""
        return self._pipeline_name

    @property
    def pipeline_run_id(self) -> str:
        """The ID of the current pipeline run."""
        return self._pipeline_run_id

    @property
    def step_name(self) -> str:
        """The name of the currently running step."""
        return self._step_name
