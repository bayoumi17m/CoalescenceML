"""
## Step Operator

While an orchestrator defines how and where your entire pipeline runs, a step 
operator defines how and where an individual step runs. An example could be
if one step within a pipeline should run on a separate environment equipped
with a GPU (like a trainer step).
"""
from coalescenceml.step_operator.base_step_operator import BaseStepOperator

__all__ = ["BaseStepOperator"]