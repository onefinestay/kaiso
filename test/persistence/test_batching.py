import pytest

from kaiso.attributes import Uuid, Integer
from kaiso.types import Entity


@pytest.fixture
def static_types(manager):
    class Thing(Entity):
        id = Uuid(unique=True)
        count = Integer()

    manager.save(Thing)

    return {
        'Thing': Thing,
    }


def test_basic(manager, static_types):
    Thing = static_types['Thing']

    thing1 = Thing()
    thing2 = Thing()
    manager.save(thing1)
    manager.save(thing2)

    ids = [obj.id for obj in (thing1, thing2)]
    result = manager.get_by_unique_attr(Thing, 'id', ids)
    result = list(result)
    assert len(result) == 2

    loaded1, loaded2 = result
    assert type(loaded1) is Thing
    assert type(loaded2) is Thing

    assert loaded1.id == thing1.id
    assert loaded2.id == thing2.id


def test_unknown_value(manager, static_types):
    Thing = static_types['Thing']

    thing1 = Thing()
    manager.save(thing1)

    ids = [thing1.id, '---']
    result = manager.get_by_unique_attr(Thing, 'id', ids)
    result = list(result)
    assert len(result) == 2

    loaded1, loaded2 = result
    assert type(loaded1) is Thing
    assert loaded2 is None


def test_bad_attr_name(manager, static_types):
    Thing = static_types['Thing']

    with pytest.raises(ValueError) as exc:
        manager.get_by_unique_attr(Thing, 'foo', '')
    assert "has no attribute" in str(exc)


def test_bad_attr(manager, static_types):
    Thing = static_types['Thing']

    with pytest.raises(ValueError) as exc:
        manager.get_by_unique_attr(Thing, 'count', '')
    assert "is not unique" in str(exc)
