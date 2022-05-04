from typing import Dict

from coalescenceml.directory import Directory
from coalescenceml.enums import ExecutionStatus, StackComponentFlavor
from coalescenceml.stack import Stack


def generate_base_validation_function(
    pipeline_name: str, step_count: int, run_count: int =1,
):
    """
    """
    def validator_function(directory: Directory):
        """ """
        pipe = directory.get_pipeline(pipeline_name)
        assert pipe

        for run in pipe.runs[-run_count:]:
            assert run.status == ExecutionStatus.COMPLETED
            assert len(run.steps) == step_count

    return validator_function


def caching_example_validation_function(directory: Directory):
    pipe = directory.get_pipeline("") # TODO: Fill in pipeline name
    assert pipe

    first_run, second_run = pipe.runs[-2:]

    # Both runs are complete
    assert first_run.status == ExecutionStatus.COMPLETED
    assert second_run.status == ExecutionStatus.COMPLETED

    # First run should have no cached steps
    for step in first_run.steps:
        assert not step.is_cached

    # Second run should have X cached steps
    assert second_run.steps[0].is_cached
    assert not second_run.steps[1].is_cached

