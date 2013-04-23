from kaiso.persistence import(
    object_to_dict, dict_to_object, get_type_relationships)
from kaiso.relationships import Relationship, InstanceOf, IsA
from kaiso.types import PersistableMeta, Entity, AttributedBase


class Foo(Entity):
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
    result = list(get_type_relationships(Entity))

    assert result == [
        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, object),
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
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, object),
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
        (PersistableMeta, InstanceOf, type),
        (AttributedBase, IsA, object),
        (AttributedBase, InstanceOf, PersistableMeta),
        (Entity, IsA, AttributedBase),
        (Entity, InstanceOf, PersistableMeta),
        (Foo, IsA, Entity),
        (Foo, InstanceOf, PersistableMeta),
        (foo, InstanceOf, Foo),
    ]
