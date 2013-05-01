import pytest

from kaiso.exceptions import DeserialisationError, UnknownType
from kaiso.attributes import String, Uuid
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.serialize import (
    get_type_relationships, get_changes,
    object_to_dict, dict_to_object, dict_to_db_values_dict)
from kaiso.types import (
    Persistable, MetaMeta, PersistableMeta, Entity, AttributedBase, Attribute)


class Foo(Entity):
    pass


class Bar(Attribute):
    pass


class Spam(Entity):
    id = String()
    ham = String(default='eggs')


def test_classes_to_dict():
    dct = object_to_dict(Entity)
    assert dct == {'__type__': 'PersistableMeta', 'id': 'Entity'}

    obj = dict_to_object(dct)
    assert obj is Entity

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'PersistableMeta', 'id': 'Foo'}

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
    assert dct == {
        '__type__': 'Bar', 'unique': True, 'required': False, 'name': None}

    # Attribute objects have default values of ``None`` for ``unique``
    # and ``required``.
    obj = dict_to_object({'__type__': 'Bar', 'unique': True, 'name': None})
    assert isinstance(obj, Bar)
    assert obj.unique is True
    assert obj.required is None
    assert obj.name is None


def test_relationship():
    dct = object_to_dict(Relationship(None, None))
    assert dct == {'__type__': 'Relationship'}

    obj = dict_to_object(dct)
    assert isinstance(obj, Relationship)


def test_obj_with_attrs():
    spam = Spam(id=None)

    dct = object_to_dict(spam, include_none=False)
    assert dct == {'__type__': 'Spam', 'ham': 'eggs'}

    dct = object_to_dict(spam, include_none=True)
    assert dct == {'__type__': 'Spam', 'id': None, 'ham': 'eggs'}

    obj = dict_to_object(dct)
    assert obj.id == spam.id
    assert obj.ham == spam.ham

    dct.pop('ham')  # removing an attr with defaults
    obj = dict_to_object(dct)
    assert obj.id == spam.id
    assert obj.ham == spam.ham


def test_dynamic_type():
    DynamicType = type('DynamicType', (PersistableMeta,), {})

    Foobar = DynamicType('Foobar', (Entity,), {})

    dct = object_to_dict(Foobar)
    assert dct == {'__type__': 'PersistableMeta', 'id': 'Foobar'}

    # since Foobar is only registered with DynamicType
    with pytest.raises(UnknownType):
        obj = dict_to_object(dct)

    obj = dict_to_object(dct, DynamicType)
    assert obj is Foobar


def test_dynamic_typed_object():
    DynamicType = type('DynamicType', (PersistableMeta,), {})

    attrs = {'id': String()}
    Foobar = DynamicType('Foobar', (Entity,), attrs)

    foo = Foobar(id='spam')
    dct = object_to_dict(foo, DynamicType)

    assert dct == {'__type__': 'Foobar', 'id': 'spam'}

    obj = dict_to_object(dct, DynamicType)
    assert isinstance(obj, Foobar)
    assert obj.id == foo.id


def test_extended_declared_type_using_dynamic_type():
    DynamicType = type('DynamicType', (PersistableMeta,), {})

    attrs = {'id': Uuid()}
    DynEntity = DynamicType('Entity', (AttributedBase,), attrs)

    foo = DynEntity()
    dct = object_to_dict(foo, DynamicType)

    assert dct == {'__type__': 'Entity', 'id': str(foo.id)}

    obj = dict_to_object(dct, DynamicType)
    assert isinstance(obj, Entity)
    assert not isinstance(obj, DynEntity)
    assert obj.id == foo.id


def test_extended_declared_type_using_declared_type():
    DynamicType = type('DynamicType', (PersistableMeta,), {})

    attrs = {'id': String()}
    DynamicType('Entity', (AttributedBase,), attrs)

    dct = object_to_dict(Entity(), DynamicType)
    assert dct == {'__type__': 'Entity', 'id': None}


def test_extended_declared_type_with_default_using_declared_type():
    DynamicType = type('DynamicType', (PersistableMeta,), {})

    attrs = {
        'id': String(default='foobar'),
        'spam': Uuid()
    }
    DynamicType('Entity', (AttributedBase,), attrs)

    dct = object_to_dict(Entity(), DynamicType)
    assert dct == {'__type__': 'Entity', 'id': 'foobar', 'spam': None}


def test_changes():
    a = {'a': 12, 'b': '34'}
    b = {'a': 1, 'b': '34'}

    changes = get_changes(a, b)
    assert changes == {'a': 1}

    with pytest.raises(KeyError):
        get_changes(a, {})


def test_attribute_types():
    uid = Uuid().default
    dct = dict_to_db_values_dict({'foo': uid, 'bar': 123})

    assert dct == {'foo': str(uid), 'bar': 123}


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
        (MetaMeta, IsA, type),
        (MetaMeta, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, MetaMeta),
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
        (MetaMeta, IsA, type),
        (MetaMeta, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, MetaMeta),
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
        (MetaMeta, IsA, type),
        (MetaMeta, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, type),
        (PersistableMeta, IsA, Persistable),
        (PersistableMeta, InstanceOf, MetaMeta),
        (AttributedBase, IsA, Persistable),
        (AttributedBase, InstanceOf, PersistableMeta),
        (Entity, IsA, AttributedBase),
        (Entity, InstanceOf, PersistableMeta),
        (Foo, IsA, Entity),
        (Foo, InstanceOf, PersistableMeta),
        (foo, InstanceOf, Foo),
    ]
