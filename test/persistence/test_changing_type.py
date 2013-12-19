import pytest

from kaiso.attributes import Uuid, String
from kaiso.exceptions import (
    NoResultFound, NoUniqueAttributeError, TypeNotPersistedError)
from kaiso.queries import get_start_clause, join_lines
from kaiso.relationships import InstanceOf
from kaiso.types import Entity


@pytest.fixture
def static_types(manager):
    class Thing(Entity):
        id = Uuid(unique=True)

    class ThingA(Thing):
        aa = String()
        same_but_different = String()

    class ThingB(Thing):
        bb = String()
        same_but_different = Uuid()

    manager.save(ThingA)
    manager.save(ThingB)

    return {
        'Thing': Thing,
        'ThingA': ThingA,
        'ThingB': ThingB,
    }


def has_property(manager, obj, prop):
    query_str = join_lines(
        "START",
        get_start_clause(obj, 'node', manager.type_registry),
        """
            return node.{}?
        """.format(prop)
    )

    properties = list(manager.query(query_str))
    return not (properties == [(None,)])


def get_instance_of_relationship(manager, obj):
    query_str = join_lines(
        "START",
        get_start_clause(obj, 'node', manager.type_registry),
        """
            match node -[instance_of:INSTANCEOF]-> ()
            return instance_of
        """
    )
    instance_of, = next(manager.query(query_str))
    return instance_of


def test_basic(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert type(new_obj) is ThingB
    assert type(retrieved) is ThingB

    # check new relationship has been created correctly
    instance_of_obj = get_instance_of_relationship(manager, new_obj)
    assert type(instance_of_obj) is InstanceOf


def test_removes_obsoleted_attributes(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert not hasattr(new_obj, 'aa')
    assert not hasattr(retrieved, 'aa')
    assert not has_property(manager, new_obj, 'aa')


def test_gets_added_attributes(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert new_obj.bb is None
    assert retrieved.bb is None


def test_keeps_common_attributes(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)
    id_ = thing_a.id

    new_obj = manager.change_instance_type(thing_a, 'ThingB')

    assert new_obj.id == id_


def test_gets_updated_values(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB', {'bb': 'bb'})
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert new_obj.bb == 'bb'
    assert retrieved.bb == 'bb'
    assert has_property(manager, new_obj, 'bb')


def test_skips_mismached_updated_values(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB', {'cc': 'cc'})
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert not hasattr(new_obj, 'cc')
    assert not hasattr(retrieved, 'cc')
    assert not has_property(manager, new_obj, 'cc')


def test_mismatching_attributes(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA(same_but_different='foo')
    manager.save(thing_a)

    with pytest.raises(ValueError):
        manager.change_instance_type(thing_a, 'ThingB')


def test_change_to_self(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingA')

    assert manager.serialize(new_obj) == manager.serialize(thing_a)


def test_change_to_unknown_type(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    with pytest.raises(TypeNotPersistedError):
        manager.change_instance_type(thing_a, 'ThingC')


def test_change_to_unsaved_type(manager, static_types):
    Thing = static_types['Thing']
    ThingA = static_types['ThingA']

    class ThingC(Thing):
        pass

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    with pytest.raises(TypeNotPersistedError):
        manager.change_instance_type(thing_a, 'ThingC')


def test_change_unsaved_instance(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA()

    with pytest.raises(NoResultFound):
        manager.change_instance_type(thing_a, 'ThingA')


def test_change_from_unsaved_type(manager, static_types):
    Thing = static_types['Thing']

    class ThingC(Thing):
        pass

    thing_c = ThingC()

    # We validate the target type, but not the source.
    # If the source type isn't persisted, there is no way that the instance can
    # be, in which case we are happy to treat this as that (unsaved instance).
    with pytest.raises(NoResultFound):
        manager.change_instance_type(thing_c, 'ThingA')


def test_change_instance_with_no_unique_attr(manager, static_types):
    class ThingC(Entity):
        pass

    manager.save(ThingC)

    thing_c = ThingC()
    manager.save(thing_c)

    with pytest.raises(NoUniqueAttributeError):
        manager.change_instance_type(thing_c, 'ThingA')


def test_change_unique_declaration(manager):
    class ThingA(Entity):
        id = Uuid(unique=True)

    class ThingB(Entity):
        id = Uuid(unique=True)

    manager.save(ThingA)
    manager.save(ThingB)

    thing = ThingA()
    manager.save(thing)

    manager.change_instance_type(thing, 'ThingB')

    assert next(manager.get_by_unique_attr(ThingA, 'id', [thing.id])) is None
    assert next(manager.get_by_unique_attr(ThingB, 'id', [thing.id]))
