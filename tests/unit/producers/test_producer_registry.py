from contextlib import ExitStack as does_not_raise

import pytest

from coalescenceml.step.exceptions import StepInterfaceError
from coalescenceml.producers.base_producer import BaseProducer
from coalescenceml.steps import step


def test_producer_with_subclassing_parameter():
    """Tests whether the steps work where one parameter subclasses one of the
    registered types"""

    class MyFloatType(float):
        pass

    @step
    def some_step() -> MyFloatType:
        return MyFloatType(3.0)

    with does_not_raise():
        some_step()()


def test_producer_with_parameter_with_more_than_one_baseclass():
    """Tests if the producer selection work where the parameter has more
    than one baseclass, however only one of the types is registered"""

    class MyOtherType:
        pass

    class MyFloatType(float, MyOtherType):
        pass

    @step
    def some_step() -> MyFloatType:
        return MyFloatType(3.0)

    with does_not_raise():
        some_step()()


def test_producer_with_parameter_with_more_than_one_conflicting_baseclass():
    """Tests the case where the output parameter is inheriting from more than
    one baseclass which have different default producers"""

    class MyFirstType:
        pass

    class MySecondType:
        pass

    class MyFirstProducer(BaseProducer):
        ASSOCIATED_TYPES = (MyFirstType,)

    class MySecondProducer(BaseProducer):
        ASSOCIATED_TYPES = (MySecondType,)

    class MyConflictingType(MyFirstType, MySecondType):
        pass

    @step
    def some_step() -> MyConflictingType:
        return MyConflictingType()

    with pytest.raises(StepInterfaceError):
        some_step()()


def test_producer_with_conflicting_parameter_and_explicit_producer():
    """Tests the case where the output parameter is inheriting from more than
    one baseclass which have different default producers but the
    producer is explicitly defined"""

    class MyFirstType:
        pass

    class MySecondType:
        pass

    class MyFirstProducer(BaseProducer):
        ASSOCIATED_TYPES = (MyFirstType,)

    class MySecondProducer(BaseProducer):
        ASSOCIATED_TYPES = (MySecondType,)

    class MyConflictingType(MyFirstType, MySecondType):
        pass

    @step
    def some_step() -> MyConflictingType:
        return MyConflictingType()

    with does_not_raise():
        some_step().with_return_producers(MyFirstProducer)()