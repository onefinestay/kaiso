import uuid

from mock import patch
from py2neo import cypher
import pytest

from kaiso.attributes import Uuid, String
from kaiso.queries import get_match_clause, join_lines
from kaiso.relationships import Relationship, IsA
from kaiso.types import Entity, collector, get_type_id


@pytest.fixture
def static_types(manager):
    class Related(Relationship):
        str_attr = String()

    class Thing(Entity):
        id = Uuid(unique=True)
        cls_attr = "spam"

    manager.save(Related)
    manager.save(Thing)

    return {
        'Related': Related,
        'Thing': Thing,
    }


def test_type_system_version(manager):
    forced_uuid = uuid.uuid4().hex

    with patch('kaiso.persistence.uuid') as patched_uuid:
        patched_uuid.uuid4().hex = forced_uuid
        manager.invalidate_type_system()

    assert manager._type_system_version() == forced_uuid

    manager.invalidate_type_system()
    assert manager._type_system_version() != forced_uuid


def test_type_system_reload(manager_factory, static_types):
    Thing = static_types['Thing']

    manager_factory(skip_setup=True).destroy()
    manager1 = manager_factory()
    manager2 = manager_factory()

    manager1.save(Thing)
    manager2.reload_types()

    type_id = get_type_id(Thing)
    assert manager2.type_registry.get_class_by_id(type_id) == Thing

    Thing.cls_attr = "cls_attr"
    manager1.save(Thing)
    manager2.reload_types()

    descriptor = manager2.type_registry.get_descriptor_by_id(type_id)
    assert "cls_attr" in descriptor.class_attributes


def test_reload_external_changes(manager, connection, static_types):
    Thing = static_types['Thing']

    manager.save(Thing)
    manager.reload_types()  # cache type registry

    # update the graph as an external manager would
    # (change a value and bump the typesystem version)
    match_clauses = (
        get_match_clause(Thing, 'Thing', manager.type_registry),
        get_match_clause(manager.type_system, 'ts', manager.type_registry),
    )
    query = join_lines(
        'MATCH',
        (match_clauses, ','),
        'SET ts.version = {version}',
        'SET Thing.cls_attr = {cls_attr}',
        'RETURN Thing'
    )
    query_params = {
        'cls_attr': 'placeholder',
        'version': str(uuid.uuid4())
    }
    cypher.execute(connection, query, query_params)

    # reloading types should see the difference
    manager.reload_types()
    descriptor = manager.type_registry.get_descriptor(Thing)
    assert "cls_attr" in descriptor.class_attributes


def test_invalidate_type_system(manager, static_types):
    Related = static_types['Related']

    with collector():
        class TypeA(Entity):
            id = Uuid(unique=True)

        class TypeB(Entity):
            attr = String(unique=True)

        class BaseType(Entity):
            pass

    manager.type_registry.register(TypeA)
    manager.type_registry.register(TypeB)
    manager.type_registry.register(BaseType)

    versions = []

    def is_distinct_version(v):
        distinct = v not in versions
        versions.append(v)
        return distinct

    v0 = manager._type_system_version()
    assert is_distinct_version(v0)

    manager.save(TypeA)  # create type
    manager.save(BaseType)
    v1 = manager._type_system_version()
    assert is_distinct_version(v1)

    manager.save(TypeA)  # save unchanged type
    assert manager._type_system_version() == v1

    type_a = TypeA()
    manager.save(type_a)  # create instance
    assert manager._type_system_version() == v1

    type_b = TypeB(attr="value")
    manager.save(TypeB)
    manager.save(type_b)  # create instance & type
    v3 = manager._type_system_version()
    assert is_distinct_version(v3)

    type_b.new_attr = "value"
    manager.save(type_b)  # add instance attribute
    assert manager._type_system_version() == v3

    type_b.new_attr = "new_value"
    manager.save(type_b)  # modify instance attribute
    assert manager._type_system_version() == v3

    del type_b.new_attr
    manager.save(type_b)  # delete instance attribute
    assert manager._type_system_version() == v3

    isa = IsA(type_a, BaseType)
    isa.base_index = 1
    manager.save(isa)  # create a type-hierarchy relationship
    v4 = manager._type_system_version()
    assert is_distinct_version(v4)

    rel = Related(type_a, type_b)
    manager.save(rel)  # create a non-type-hierarchy relationship
    assert manager._type_system_version() == v4

    manager.update_type(TypeA, (BaseType,))  # reparent
    v5 = manager._type_system_version()
    assert is_distinct_version(v5)

    # update_type reloads the type hierarchy, so refresh references
    TypeA = manager.type_registry.get_class_by_id('TypeA')
    TypeB = manager.type_registry.get_class_by_id('TypeB')
    BaseType = manager.type_registry.get_class_by_id('BaseType')

    manager.delete(isa)  # delete a type-hierarchy relationship
    v6 = manager._type_system_version()
    assert is_distinct_version(v6)

    manager.delete(rel)  # delete a non-type-hierarchy relationship
    assert manager._type_system_version() == v6

    manager.delete(TypeA)  # delete type
    v7 = manager._type_system_version()
    assert is_distinct_version(v7)

    manager.delete(type_a)  # delete instance
    assert manager._type_system_version() == v7

    TypeB.cls_attr = "value"
    manager.save(TypeB)  # add class attribute
    v8 = manager._type_system_version()
    assert is_distinct_version(v8)

    TypeB.cls_attr = "new_value"
    manager.save(TypeB)  # modify class attribute
    v9 = manager._type_system_version()
    assert is_distinct_version(v9)

    del TypeB.cls_attr
    manager.save(TypeB)  # delete class attribute
    v10 = manager._type_system_version()
    assert is_distinct_version(v10)


def test_cached_type_system_keeps_types_in_db(manager_factory):
    # regression test
    manager = manager_factory(skip_setup=True)
    manager.destroy()

    manager1 = manager_factory()

    class Foo(Entity):
        pass

    manager1.save(Foo)
    manager1.reload_types()

    manager2 = manager_factory()

    foo = Foo()
    manager2.save(foo)
