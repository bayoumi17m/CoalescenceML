import pytest

from coalescenceml.pipeline import pipeline

@pytest.fixture
def one_step_pipeline():
    """Pytest fixture that returns a pipeline which takes a single step
    named `step_`."""

    @pipeline
    def _pipeline(step_):
        step_()

    return _pipeline