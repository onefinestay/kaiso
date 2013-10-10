import pytest

from kaiso.attributes import String
from kaiso.exceptions import TypeAlreadyRegistered, TypeAlreadyCollected
from kaiso.types import Entity, TypeRegistry, get_declaring_class


def test_register_duplicate():

    type_registry = TypeRegistry()

    with pytest.raises(TypeAlreadyRegistered):
        # this declares a dynamic type
        type_registry.create_type('Duplicate', (Entity,), {})
        type_registry.create_type('Duplicate', (Entity,), {})


def test_collect_duplicate():

    # this declares a static class
    class Duplicate(Entity):
        pass

    with pytest.raises(TypeAlreadyCollected):
        # this also declares a static class
        type("Duplicate", (Entity,), {})


def test_get_declaring_class():

    class X(object):
        foo = String()

    class Y(X):
        foo = String()
        bar = String()

    class Z(Y):
        bar = String()

    # prefer subclass
    assert get_declaring_class(X, "foo") == X
    assert get_declaring_class(X, "bar") is None
    assert get_declaring_class(Z, "foo") == Y
    assert get_declaring_class(Z, "bar") == Z

    # prefer parent
    assert get_declaring_class(X, "foo", prefer_subclass=False) == X
    assert get_declaring_class(X, "bar", prefer_subclass=False) is None
    assert get_declaring_class(Z, "foo", prefer_subclass=False) == X
    assert get_declaring_class(Z, "bar", prefer_subclass=False) == Y
