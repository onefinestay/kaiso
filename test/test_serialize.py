import pytest

from kaiso.exceptions import DeserialisationError
from kaiso.attributes import String, Uuid, Choice
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.serialize import (
    get_type_relationships, get_changes, dict_to_db_values_dict)
from kaiso.types import (
    Persistable, PersistableType, Entity, AttributedBase, Attribute)


def test_classes_to_dict(type_registry):
    class Foo(Entity):
        pass

    dct = type_registry.object_to_dict(Entity)
    assert dct == {'__type__': 'PersistableType', 'id': 'Entity'}

    obj = type_registry.dict_to_object(dct)
    assert obj is Entity

    dct = type_registry.object_to_dict(Foo)
    assert dct == {'__type__': 'PersistableType', 'id': 'Foo'}

    obj = type_registry.dict_to_object(dct)
    assert obj is Foo


def test_objects(type_registry):
    class Foo(Entity):
        pass

    dct = type_registry.object_to_dict(Entity())
    assert dct == {'__type__': 'Entity'}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Entity)

    dct = type_registry.object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Foo)


def test_attribute(type_registry):
    """ Verify (de)serialization of Attributes.
    """
    class Bar(Attribute):
        pass

    # Attribute dicts always contain both ``unique`` and ``required`` keys.
    attr = Bar(unique=True)
    dct = type_registry.object_to_dict(attr)
    assert dct == {
        '__type__': 'Bar', 'unique': True, 'required': False, 'name': None}

    # Attribute objects have default values of ``None`` for ``unique``
    # and ``required``.
    obj = type_registry.dict_to_object(
        {'__type__': 'Bar', 'unique': True, 'name': None})
    assert isinstance(obj, Bar)
    assert obj.unique is True
    assert obj.required is None
    assert obj.name is None


def test_choices(type_registry):

    attr = Choice('ham', 'spam', 'eggs')
    dct = type_registry.object_to_dict(attr)

    assert dct == {
        '__type__': 'Choice', 'name': None,
        'unique': False, 'required': False, 'default': None,
        'choices': ['ham', 'spam', 'eggs']}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Choice)
    assert obj.choices == ('ham', 'spam', 'eggs')


def test_relationship(type_registry):

    dct = type_registry.object_to_dict(Relationship(None, None))
    assert dct == {'__type__': 'Relationship'}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Relationship)


def test_obj_with_attrs(type_registry):
    class Spam(Entity):
        id = String()
        ham = String(default='eggs')

    spam = Spam(id=None)

    dct = type_registry.object_to_dict(spam, for_db=True)
    assert dct == {'__type__': 'Spam', 'ham': 'eggs'}

    dct = type_registry.object_to_dict(spam, for_db=False)
    assert dct == {'__type__': 'Spam', 'id': None, 'ham': 'eggs'}

    obj = type_registry.dict_to_object(dct)
    assert obj.id == spam.id
    assert obj.ham == spam.ham

    dct.pop('ham')  # removing an attr
    obj = type_registry.dict_to_object(dct)
    assert obj.id == spam.id
    assert obj.ham is None


def test_dynamic_type(type_registry):
    Foobar = type_registry.create_type('Foobar', (Entity,), {})

    dct = type_registry.object_to_dict(Foobar)
    assert dct == {'__type__': 'PersistableType', 'id': 'Foobar'}

    obj = type_registry.dict_to_object(dct)
    assert obj is Foobar


def test_dynamic_typed_object(type_registry):

    attrs = {'id': String()}
    Foobar = type_registry.create_type('Foobar', (Entity,), attrs)

    foo = Foobar(id='spam')
    dct = type_registry.object_to_dict(foo)

    assert dct == {'__type__': 'Foobar', 'id': 'spam'}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Foobar)
    assert obj.id == foo.id


def test_extend_declared_type_using_dynamic_type(type_registry):

    attrs = {'id': Uuid()}
    DynEntity = type_registry.create_type('Entity', (AttributedBase,), attrs)

    foo = DynEntity()
    dct = type_registry.object_to_dict(foo)

    assert dct == {'__type__': 'Entity', 'id': str(foo.id)}

    obj = type_registry.dict_to_object(dct)
    assert isinstance(obj, Entity)
    assert not isinstance(obj, DynEntity)
    assert obj.id == foo.id


def test_extend_declared_type_using_declared_type(type_registry):
    attrs = {'id': String()}
    type_registry.create_type('Entity', (AttributedBase,), attrs)

    dct = type_registry.object_to_dict(Entity())
    assert dct == {'__type__': 'Entity', 'id': None}


def test_extend_declared_type_with_default_using_declared_type(type_registry):
    attrs = {
        'id': String(default='foobar'),
        'spam': Uuid()
    }
    type_registry.create_type('Entity', (AttributedBase,), attrs)

    dct = type_registry.object_to_dict(Entity())
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


def test_missing_info(type_registry):
    with pytest.raises(DeserialisationError):
        type_registry.dict_to_object({})


def test_IsA_and_InstanceOf_type_relationships(temporary_static_types):
    """ Testing type hierarchy creation.

    We don't want to test all the types in their relationships,
    but only the set of types which are persisted with IsA and InstanceOf
    relationships.
    """
    class Foo(Entity):
        pass

    result = list(get_type_relationships(Entity))

    assert result == [
        (object, (InstanceOf, 0), type),
        (type, (IsA, 0), object),
        (type, (InstanceOf, 0), type),
        (PersistableType, (IsA, 0), type),
        (Persistable, (IsA, 0), object),
        (Persistable, (InstanceOf, 0), type),
        (PersistableType, (IsA, 1), Persistable),
        (PersistableType, (InstanceOf, 0), type),
        (AttributedBase, (IsA, 0), Persistable),
        (AttributedBase, (InstanceOf, 0), PersistableType),
        (Entity, (IsA, 0), AttributedBase),
        (Entity, (InstanceOf, 0), PersistableType),
    ]

    pers = Entity()
    result = list(get_type_relationships(pers))

    assert result == [
        (object, (InstanceOf, 0), type),
        (type, (IsA, 0), object),
        (type, (InstanceOf, 0), type),
        (PersistableType, (IsA, 0), type),
        (Persistable, (IsA, 0), object),
        (Persistable, (InstanceOf, 0), type),
        (PersistableType, (IsA, 1), Persistable),
        (PersistableType, (InstanceOf, 0), type),
        (AttributedBase, (IsA, 0), Persistable),
        (AttributedBase, (InstanceOf, 0), PersistableType),
        (Entity, (IsA, 0), AttributedBase),
        (Entity, (InstanceOf, 0), PersistableType),
        (pers, (InstanceOf, 0), Entity),
    ]

    foo = Foo()
    result = list(get_type_relationships(foo))

    assert result == [
        (object, (InstanceOf, 0), type),
        (type, (IsA, 0), object),
        (type, (InstanceOf, 0), type),
        (PersistableType, (IsA, 0), type),
        (Persistable, (IsA, 0), object),
        (Persistable, (InstanceOf, 0), type),
        (PersistableType, (IsA, 1), Persistable),
        (PersistableType, (InstanceOf, 0), type),
        (AttributedBase, (IsA, 0), Persistable),
        (AttributedBase, (InstanceOf, 0), PersistableType),
        (Entity, (IsA, 0), AttributedBase),
        (Entity, (InstanceOf, 0), PersistableType),
        (Foo, (IsA, 0), Entity),
        (Foo, (InstanceOf, 0), PersistableType),
        (foo, (InstanceOf, 0), Foo),
    ]
