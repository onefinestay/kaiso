import pytest

from kaiso.attributes import Uuid, Incoming, Outgoing
from kaiso.exceptions import MultipleObjectsFound, NoResultFound
from kaiso.relationships import Relationship
from kaiso.types import Entity


@pytest.fixture
def static_types(manager):
    class Contains(Relationship):
        id = Uuid(unique=True)

    class Box(Entity):
        id = Uuid(unique=True)

        contains = Outgoing(Contains)
        contained_within = Incoming(Contains)

    manager.save(Box)
    manager.save(Contains)

    return {
        'Box': Box,
        'Contains': Contains,
    }


def test_rel_attributes(manager, static_types):
    Box = static_types['Box']
    Contains = static_types['Contains']

    box1 = Box()
    box2 = Box()
    contains = Contains(box1, box2)

    manager.save(box1)
    manager.save(box2)
    manager.save(contains)

    assert [b.id for b in box1.contains] == [box2.id]
    assert box2.contained_within.one().id == box1.id


def test_rel_one_missing(manager, static_types):
    Box = static_types['Box']

    box = Box()

    manager.save(box)

    with pytest.raises(NoResultFound):
        box.contains.one()


def test_rel_one_multiple(manager, static_types):
    Box = static_types['Box']
    Contains = static_types['Contains']

    parent = Box()
    child1 = Box()
    child2 = Box()

    contains1 = Contains(parent, child1)
    contains2 = Contains(parent, child2)

    manager.save(parent)
    manager.save(child1)
    manager.save(child2)
    manager.save(contains1)
    manager.save(contains2)

    with pytest.raises(MultipleObjectsFound):
        parent.contains.one()


def test_empty_rel_attributes(manager, static_types):
    Box = static_types['Box']

    box = Box()

    manager.save(box)
    # TODO: should the attr support len()
    assert len(list(box.contains)) == 0
    assert box.contained_within.first() is None


def test_many_children(manager, static_types):
    Box = static_types['Box']
    Contains = static_types['Contains']

    parent = Box()
    child1 = Box()
    child2 = Box()

    contains1 = Contains(parent, child1)
    contains2 = Contains(parent, child2)

    manager.save(parent)
    manager.save(child1)
    manager.save(child2)
    manager.save(contains1)
    manager.save(contains2)

    assert len(list(parent.contains)) == 2
    assert parent.contains.first().id in [child1.id, child2.id]


def test_reference_relationship_itself(manager, static_types):
    Box = static_types['Box']
    Contains = static_types['Contains']

    parent = Box()
    child = Box()

    contains = Contains(parent, child)

    manager.save(parent)
    manager.save(child)
    manager.save(contains)

    fetched = manager.get(Box, id=str(child.id))
    fetched_rel = next(fetched.contained_within.relationships)

    assert fetched_rel.id == contains.id
