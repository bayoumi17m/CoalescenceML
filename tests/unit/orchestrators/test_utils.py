import datetime as dt
import json
from typing import Optional

import pytest
from pydantic.main import BaseModel
from tfx.proto.orchestration.pipeline_pb2 import PipelineNode
from tfx.orchestration.portable.data_types import ExecutionInfo
from tfx.proto.orchestration.pipeline_pb2 import PipelineInfo

from coalescenceml.enums import MetadataContextFlavor, StackComponentFlavor
from coalescenceml.orchestrator.utils import (
    add_runtime_configuration_to_node,
    get_cache_status,
)
from coalescenceml.directory import Directory
from coalescenceml.step import step
from coalescenceml.step.utils import (
    INTERNAL_EXECUTION_PARAMETER_PREFIX,
    PARAM_PIPELINE_PARAMETER_NAME,
)


def test_get_cache_status_raises_no_error_when_none_passed():
    """Ensure get_cache_status raises no error when None is passed."""
    try:
        get_cache_status(None)
    except AttributeError:
        pytest.fail("`get_cache_status()` raised an `AttributeError`")


def test_get_cache_status_works_when_running_pipeline_twice():
    """Check that steps are cached when a pipeline is run twice successively."""
    from coalescenceml.pipeline import pipeline
    from coalescenceml.step import step

    @step
    def step_one():
        return 1

    @pipeline
    def some_pipeline(
        step_one,
    ):
        step_one()

    pipeline = some_pipeline(
        step_one=step_one(),
    )

    pipeline.run()
    pipeline.run()

    pipeline = Directory().get_pipeline("some_pipeline")
    first_run = pipeline.runs[-2]
    second_run = pipeline.runs[-1]

    step_name_param = (
        INTERNAL_EXECUTION_PARAMETER_PREFIX + PARAM_PIPELINE_PARAMETER_NAME
    )
    properties_param = json.dumps("step_one")
    pipeline_id = pipeline.name
    first_run_id = first_run.name
    second_run_id = second_run.name
    first_run_execution_object = ExecutionInfo(
        exec_properties={step_name_param: properties_param},
        pipeline_info=PipelineInfo(id=pipeline_id),
        pipeline_run_id=first_run_id,
    )
    second_run_execution_object = ExecutionInfo(
        exec_properties={step_name_param: properties_param},
        pipeline_info=PipelineInfo(id=pipeline_id),
        pipeline_run_id=second_run_id,
    )

    assert get_cache_status(first_run_execution_object) is False
    assert get_cache_status(second_run_execution_object) is True

def test_pipeline_storing_stack_in_the_metadata_store(one_step_pipeline):
    """Tests that returning an object of a type that wasn't specified (either
    directly or as part of the `Output` tuple annotation) raises an error."""

    @step
    def some_step_1() -> int:
        return 3

    pipeline_ = one_step_pipeline(some_step_1())
    pipeline_.run()

    dir_ = Directory()

    stack = dir_.get_stack(dir_.active_stack_name)
    metadata_store = stack.metadata_store
    stack_contexts = metadata_store.store.get_contexts_by_type(
        MetadataContextFlavor.STACK.value
    )

    assert len(stack_contexts) == 1

    assert stack_contexts[0].custom_properties[
        StackComponentFlavor.ORCHESTRATOR.value
    ].string_value == stack.orchestrator.json(sort_keys=True)
    assert stack_contexts[0].custom_properties[
        StackComponentFlavor.ARTIFACT_STORE.value
    ].string_value == stack.artifact_store.json(sort_keys=True)
    assert stack_contexts[0].custom_properties[
        StackComponentFlavor.METADATA_STORE.value
    ].string_value == stack.metadata_store.json(sort_keys=True)


def test_pydantic_object_to_metadata_context():
    class Unjsonable:
        def __init__(self):
            self.value = "value"

    class StringAttributes(BaseModel):
        b: str
        a: str

    class DateTimeAttributes(BaseModel):
        t: dt.datetime
        d: Optional[dt.date]

    class MixedAttributes(BaseModel):
        class Config:
            arbitrary_types_allowed = True

        s: str
        f: float
        i: int
        b: bool
        l: list
        u: Unjsonable

    # straight-forward fully serializable object

    node1 = PipelineNode()
    obj1 = StringAttributes(b="bob", a="alice")
    add_runtime_configuration_to_node(node1, {"key": obj1})
    print(f"methods: {dir(node1.contexts.contexts)}")
    ctx1 = node1.contexts.contexts[0]
    assert ctx1.type.name == "stringattributes"
    assert ctx1.name.field_value.string_value == str(
        hash('{"a": "alice", "b": "bob"}')
    )

    # object with serialization difficulties
    obj2 = MixedAttributes(
        s="steve", f=3.14, i=42, b=True, l=[1, 2], u=Unjsonable()
    )

    node2 = PipelineNode()
    add_runtime_configuration_to_node(
        node2, dict(k=obj2, ignore_unserializable_fields=True)
    )
    ctx2 = node2.contexts.contexts[0]
    assert ctx2.type.name == "mixedattributes"
    assert ctx2.name.field_value.string_value.startswith("MixedAttributes")
    assert "s" in ctx2.properties.keys()
    assert ctx2.properties.get("b").field_value.string_value == "true"
    assert ctx2.properties.get("l").field_value.string_value == "[1, 2]"
    assert "u" not in ctx2.properties.keys()

    node3 = PipelineNode()
    with pytest.raises(TypeError):
        add_runtime_configuration_to_node(node3, {"k": obj2})

    # use pydantics serialization magic

    obj4 = DateTimeAttributes(
        t=dt.datetime(2022, 10, 20, 16, 42, 5), d=dt.date(2012, 12, 20)
    )
    node4 = PipelineNode()
    add_runtime_configuration_to_node(node4, dict(k=obj4))
    ctx4 = node4.contexts.contexts[0]
    assert ctx4.type.name == "datetimeattributes"
    assert ctx4.properties.get("d").field_value.string_value == '"2012-12-20"'
    assert (
        ctx4.properties.get("t").field_value.string_value
        == '"2022-10-20T16:42:05"'
    )