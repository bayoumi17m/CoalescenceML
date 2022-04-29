from coalescenceml.utils.singleton import SingletonMetaClass


class SingletonClass(metaclass=SingletonMetaClass):
    pass


def test_singleton_class_only_create_one_instance():
    """Tests that a class with SingletonMetaClass only creates 1 instances."""
    assert SingletonClass() is SingletonClass()


def test_singleton_class_init_used_once(mocker):
    """Test that singleton __init__ method is called only once."""
    mocker.patch.object(SingletonClass, "__init__", return_value=None)

    # Clear instance
    SingletonClass._clear()
    SingletonClass()
    SingletonClass.__init__.assert_called_once()
    SingletonClass()
    SingletonClass.__init__.assert_called_once()


def test_singleton_clearing():
    """Test that singleton can be cleared by calling `_clear()`."""
    instance = SingletonClass()

    SingletonClass._clear()

    assert instance is not SingletonClass()


def test_singleton_metaclass_multiple_classes():
    """Test that multiple classes can use SingletonMetaClass and instances don't mix"""
    class SecondSingletonClass(metaclass=SingletonMetaClass):
        pass

    assert SingletonClass() is not SecondSingletonClass()
    assert type(SingletonClass()) is SingletonClass
    assert type(SecondSingletonClass()) is SecondSingletonClass
