from kaiso.persistence import(
    object_to_dict, dict_to_object, get_type_relationships)
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.types import PersistableType, Persistable, AttributedBase


class Foo(Persistable):
    pass


def test_types_to_dict():
    dct = object_to_dict(PersistableType)
    assert dct == {'__type__': 'type', 'name': 'PersistableType'}

    obj = dict_to_object(dct)
    assert obj is PersistableType


def test_classes_to_dict():
    dct = object_to_dict(Persistable)
    assert dct == {'__type__': 'PersistableType', 'name': 'Persistable'}

    obj = dict_to_object(dct)
    assert obj is Persistable

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'PersistableType', 'name': 'Foo'}

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
        (AttributedBase, IsA, object),
        (AttributedBase, InstanceOf, PersistableType),
        (Persistable, IsA, AttributedBase),
        (Persistable, InstanceOf, PersistableType),
        (Foo, IsA, Persistable),
        (Foo, InstanceOf, PersistableType),
        (foo, InstanceOf, Foo),
    ]
