import pytest

from kaiso.attributes import (
    Uuid, Decimal, String)
from kaiso.exceptions import (
    NoResultFound, NoUniqueAttributeError, TypeNotPersistedError)
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
        same_but_different = Decimal()

    manager.save(ThingA)
    manager.save(ThingB)

    return {
        'Thing': Thing,
        'ThingA': ThingA,
        'ThingB': ThingB,
    }


def test_basic(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert type(new_obj) is ThingB
    assert type(retrieved) is ThingB


def test_removes_obsoleted_attributes(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert not hasattr(new_obj, 'aa')
    assert not hasattr(retrieved, 'aa')


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


def test_skips_mismached_updated_values(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB', {'cc': 'cc'})
    retrieved = manager.get(ThingB, id=thing_a.id)

    assert not hasattr(new_obj, 'cc')
    assert not hasattr(retrieved, 'cc')


def test_mismatching_attributes(manager, static_types):
    ThingA = static_types['ThingA']

    thing_a = ThingA(same_but_different='foo')
    manager.save(thing_a)

    # TODO: should to_python catch e.g. decimal.InvalidOperation and raise
    # ValueError?
    with pytest.raises(Exception):
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
