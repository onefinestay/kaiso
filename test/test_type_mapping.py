import pytest

from kaiso.exceptions import DeserialisationError
from kaiso.persistence import(
    object_to_dict, dict_to_object, get_type_relationships)
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.types import (
    Persistable, PersistableMeta, Entity, AttributedBase, Attribute)


class Foo(Entity):
    pass


class Bar(Attribute):
    pass


def test_types_to_dict():
    dct = object_to_dict(PersistableMeta)
    assert dct == {'__type__': 'type', 'name': 'PersistableMeta'}

    obj = dict_to_object(dct)
    assert obj is PersistableMeta


def test_classes_to_dict():
    dct = object_to_dict(Entity)
    assert dct == {'__type__': 'PersistableMeta', 'name': 'Entity'}

    obj = dict_to_object(dct)
    assert obj is Entity

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'PersistableMeta', 'name': 'Foo'}

    obj = dict_to_object(dct)
    assert obj is Foo


def test_objects():
    dct = object_to_dict(Entity())
    assert dct == {'__type__': 'Entity'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Entity)

    dct = object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Foo)


def test_attribute():
    attr = Bar(unique=True)
    dct = object_to_dict(attr)
    assert dct == {'__type__': 'Bar', 'unique': True}

    obj = dict_to_object({'__type__': 'Bar', 'unique': True})
    assert isinstance(obj, Bar)
    assert obj.unique is True


def test_relationship():
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


def test_missing_info():
    with pytest.raises(DeserialisationError):
        dict_to_object({})


def test_IsA_and_InstanceOf_type_relationships():
    """ Testing type hierarchy creation.

    We don't want to test all the types in their relationships,
    but only the set of types which are persisted with IsA and InstanceOf
    relationships.
    """
    result = list(get_type_relationships(Entity))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, Persistable),
        (AttributedBase, InstanceOf, PersistableMeta),
        (Entity, IsA, AttributedBase),
        (Entity, InstanceOf, PersistableMeta),
    ]

    pers = Entity()
    result = list(get_type_relationships(pers))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, Persistable),
        (AttributedBase, InstanceOf, PersistableMeta),
        (Entity, IsA, AttributedBase),
        (Entity, InstanceOf, PersistableMeta),
        (pers, InstanceOf, Entity),
    ]

    foo = Foo()
    result = list(get_type_relationships(foo))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, Persistable),
        (AttributedBase, InstanceOf, PersistableMeta),
        (Entity, IsA, AttributedBase),
        (Entity, InstanceOf, PersistableMeta),
        (Foo, IsA, Entity),
        (Foo, InstanceOf, PersistableMeta),
        (foo, InstanceOf, Foo),
    ]
