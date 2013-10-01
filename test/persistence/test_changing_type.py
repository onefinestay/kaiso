import pytest

from kaiso.attributes import (
    Uuid, Integer, String)
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
        same_but_different = Integer()

    manager.save(ThingA)
    manager.save(ThingB)

    return {
        'ThingA': ThingA,
        'ThingB': ThingB,
    }


def test_basic(manager, static_types):
    ThingA = static_types['ThingA']
    ThingB = static_types['ThingB']

    thing_a = ThingA(aa='aa')
    manager.save(thing_a)

    new_obj = manager.change_instance_type(thing_a, 'ThingB')

    assert type(new_obj) is ThingB
    assert not hasattr(new_obj, 'aa')
    assert new_obj.bb is None

    retrieved = manager.get(ThingB, id=thing_a.id)
    assert type(retrieved) is ThingB
    assert not hasattr(retrieved, 'aa')
    assert retrieved.bb is None
