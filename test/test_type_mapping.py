import pytest

from kaiso.exceptions import DeserialisationError
from kaiso.persistence import(
    object_to_dict, dict_to_object, get_type_relationships)
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.types import (
    Persistable, PersistableMeta, Entity, AttributedBase, Attribute)
from kaiso.attributes import DefaultableAttribute


class Foo(Entity):
    pass


class Bar(Attribute):
    pass


class Baz(DefaultableAttribute):
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
    """ Verify (de)serialization of Attributes.
    """
    # Attribute dicts always contain both ``unique`` and ``required`` keys.
    attr = Bar(unique=True)
    dct = object_to_dict(attr)
    assert dct == {'__type__': 'Bar', 'unique': True, 'required': False}

    # Attribute objects have default values of ``None`` for ``unique``
    # and ``required``.
    obj = dict_to_object({'__type__': 'Bar', 'unique': True})
    assert isinstance(obj, Bar)
    assert obj.unique is True
    assert obj.required is None


def test_defaultable_attribute():
    """ Verify (de)serialization of DefaultableAttributes.
    """
    # DefaultableAttribute dicts always contain ``unique`` and ``required``
    # keys, but don't contain a ``default`` key unless it has a value.
    attr = Baz(required=True)
    dct = object_to_dict(attr)
    assert dct == {'__type__': 'Baz', 'unique': False, 'required': True}

    # DefaultableAttribute objects also have a ``default`` attribute, equal to
    # ``None`` if unset.
    obj = dict_to_object({'__type__': 'Baz', 'required': True})
    assert isinstance(obj, Baz)
    assert obj.unique is None
    assert obj.required is True
    assert obj.default is None


def test_defaultable_attribute_with_value():
    """ Verify (de)serialization of DefaultableAttributes with values.
    """
    # DefaultableAttributes dicts always contain ``unique`` and ``required``
    # keys, and will contain a ``default`` key if it has a value.
    value = "foo"
    attr = Baz(default=value)
    dct = object_to_dict(attr)
    assert dct == {'__type__': 'Baz', 'unique': False, 'required': False,
                   'default': value}

    # DefaultableAttribute objects must have a ``default`` attribute
    obj = dict_to_object({'__type__': 'Baz', 'default': 'foo',
                          'unique': False, 'required': False, })
    assert isinstance(obj, Baz)
    assert obj.unique is False
    assert obj.required is False
    assert obj.default == value


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
