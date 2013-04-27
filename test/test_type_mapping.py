import pytest

from kaiso.exceptions import DeserialisationError
from kaiso.persistence import get_type_relationships
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.types import (
    Persistable, MetaMeta, PersistableMeta, Entity, AttributedBase, Attribute)


class Foo(Entity):
    pass


class Bar(Attribute):
    pass


@pytest.mark.usefixtures('storage')
def test_classes_to_dict(storage):
    dct = storage.object_to_dict(Entity)
    assert dct == {'__type__': 'PersistableMeta', 'id': 'Entity'}

    obj = storage.dict_to_object(dct)
    assert obj is Entity

    dct = storage.object_to_dict(Foo)
    assert dct == {'__type__': 'PersistableMeta', 'id': 'Foo'}

    obj = storage.dict_to_object(dct)
    assert obj is Foo


@pytest.mark.usefixtures('storage')
def test_objects(storage):
    dct = storage.object_to_dict(Entity())
    assert dct == {'__type__': 'Entity'}

    obj = storage.dict_to_object(dct)
    assert isinstance(obj, Entity)

    dct = storage.object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}

    obj = storage.dict_to_object(dct)
    assert isinstance(obj, Foo)


@pytest.mark.usefixtures('storage')
def test_attribute(storage):
    attr = Bar(unique=True)
    dct = storage.object_to_dict(attr)
    assert dct == {'__type__': 'Bar', 'unique': True}

    obj = storage.dict_to_object({'__type__': 'Bar', 'unique': True})
    assert isinstance(obj, Bar)
    assert obj.unique is True


@pytest.mark.usefixtures('storage')
def test_relationship(storage):
    dct = storage.object_to_dict(Relationship(None, None))
    assert dct == {'__type__': 'Relationship'}

    obj = storage.dict_to_object(dct)
    assert isinstance(obj, Relationship)


@pytest.mark.usefixtures('storage')
def test_missing_info(storage):
    with pytest.raises(DeserialisationError):
        storage.dict_to_object({})


@pytest.mark.usefixtures('storage')
def test_IsA_and_InstanceOf_type_relationships(storage):
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
