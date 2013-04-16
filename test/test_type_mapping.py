from orp.persistence import(
    object_to_dict, dict_to_object, get_type_relationships)
from orp.relationships import Relationship, InstanceOf, IsA
from orp.types import PersistableType, Persistable, AttributedBase


class FooType(PersistableType):
    pass


class Foo(object):
    __metaclass__ = FooType


def test_types_to_dict():
    dct = object_to_dict(PersistableType)
    assert dct == {'__type__': 'type', 'name': 'PersistableType'}

    obj = dict_to_object(dct)
    assert obj is PersistableType

    dct = object_to_dict(FooType)
    assert dct == {'__type__': 'type', 'name': 'FooType'}

    obj = dict_to_object(dct)
    assert obj is FooType


def test_classes_to_dict():
    dct = object_to_dict(Persistable)
    assert dct == {'__type__': 'PersistableType', 'name': 'Persistable'}

    obj = dict_to_object(dct)
    assert obj is Persistable

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'FooType', 'name': 'Foo'}

    obj = dict_to_object(dct)
    assert obj is Foo


def test_objects():
    dct = object_to_dict(Persistable())
    assert dct == {'__type__': 'Persistable'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Persistable)

    dct = object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Foo)


def test_relationship_to_dict():
    dct = object_to_dict(Relationship(None, None))
    assert dct == {'__type__': 'Relationship'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Relationship)


def test_base_types():
    dct = object_to_dict(object)
    assert dct == {'__type__': 'type', 'name': 'object'}

    obj = dict_to_object(dct)
    assert obj is object

    dct = object_to_dict(type)
    assert dct == {'__type__': 'type', 'name': 'type'}

    obj = dict_to_object(dct)
    assert obj is type

    dct = object_to_dict(object())
    assert dct == {'__type__': 'object'}

    obj = dict_to_object(dct)
    assert isinstance(obj, object)


def test_type_relationships():
    result = list(get_type_relationships(Persistable))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (AttributedBase, IsA, object),
        (AttributedBase, InstanceOf, PersistableType),
        (Persistable, IsA, AttributedBase),
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
        (AttributedBase, IsA, object),
        (AttributedBase, InstanceOf, PersistableType),
        (Persistable, IsA, AttributedBase),
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


