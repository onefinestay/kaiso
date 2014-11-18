import pytest

from kaiso.attributes import String
from kaiso.exceptions import TypeAlreadyRegistered, TypeAlreadyCollected
from kaiso.types import Entity, Relationship, TypeRegistry, get_declaring_class


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
        baz = False

    class Z(Y):
        bar = String()
        baz = False

    # prefer subclass
    assert get_declaring_class(X, "foo") == X
    assert get_declaring_class(X, "bar") is None
    assert get_declaring_class(Z, "foo") == Y
    assert get_declaring_class(Z, "bar") == Z
    assert get_declaring_class(Z, "baz") == Z

    # prefer parent
    assert get_declaring_class(X, "foo", prefer_subclass=False) == X
    assert get_declaring_class(X, "bar", prefer_subclass=False) is None
    assert get_declaring_class(Z, "foo", prefer_subclass=False) == X
    assert get_declaring_class(Z, "bar", prefer_subclass=False) == Y
    assert get_declaring_class(Z, "baz", prefer_subclass=False) == Y


def test_relationship_case_sensitive_collection():
    class Foo(Relationship):
        pass

    with pytest.raises(TypeAlreadyCollected):
        class FoO(Relationship):
            pass


def test_relationship_cannot_have_unique_attrs():
    with pytest.raises(TypeError) as exc:
        class Foo(Relationship):
            id = String(unique=True)
    assert "may not have unique attributes" in str(exc)
