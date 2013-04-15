from orp.types import PersistableType, Persistable
from orp.relationships import Relationship, InstanceOf, IsA
from orp.persistence import (
    object_to_dict, get_type_relationships, types_to_query)


class FooType(PersistableType):
    pass


class Foo(object):
    __metaclass__ = FooType


def test_types_to_dict():
    dct = object_to_dict(PersistableType)
    assert dct == {'__type__': 'PersistableType', 'name': 'PersistableType'}

    dct = object_to_dict(FooType)
    assert dct == {'__type__': 'FooType', 'name': 'FooType'}


def test_classes_to_dict():
    dct = object_to_dict(Persistable)
    assert dct == {'__type__': 'PersistableType', 'name': 'Persistable'}

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'FooType', 'name': 'Foo'}


def test_objects_to_dict():
    dct = object_to_dict(Persistable())
    assert dct == {'__type__': 'Persistable'}

    dct = object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}


def test_relationship_to_dict():
    dct = object_to_dict(Relationship(None, None))
    assert dct == {'__type__': 'Relationship'}


def test_base_types():
    dct = object_to_dict(object)
    assert dct == {'__type__': 'type', 'name': 'object'}

    dct = object_to_dict(type)
    assert dct == {'__type__': 'type', 'name': 'type'}

    dct = object_to_dict(object())
    assert dct == {'__type__': 'object'}


def test_type_relationships():
    result = list(get_type_relationships(Persistable))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, PersistableType),
    ]

    pers = Persistable()
    result = list(get_type_relationships(pers))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, PersistableType),
        (pers, InstanceOf, Persistable),
    ]

    foo = Foo()
    result = list(get_type_relationships(foo))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (FooType, IsA, PersistableType),
        (FooType, InstanceOf, type),
        (Foo, IsA, object),
        (Foo, InstanceOf, FooType),
        (foo, InstanceOf, Foo),
    ]


