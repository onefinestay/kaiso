import pytest

from kaiso.attributes import Uuid, Incoming, Outgoing
from kaiso.exceptions import MultipleObjectsFound, NoResultFound
from kaiso.relationships import Relationship
from kaiso.types import Entity


class Contains(Relationship):
    id = Uuid(unique=True)


class Box(Entity):
    id = Uuid(unique=True)

    contains = Outgoing(Contains)
    contained_within = Incoming(Contains)


@pytest.mark.usefixtures('storage')
def test_rel_attributes(storage):
    box1 = Box()
    box2 = Box()
    contains = Contains(box1, box2)

    storage.save(box1)
    storage.save(box2)
    storage.save(contains)

    assert [b.id for b in box1.contains] == [box2.id]
    assert box2.contained_within.one().id == box1.id


@pytest.mark.usefixtures('storage')
def test_rel_one_missing(storage):
    box = Box()

    storage.save(box)

    with pytest.raises(NoResultFound):
        box.contains.one()


@pytest.mark.usefixtures('storage')
def test_rel_one_multiple(storage):
    parent = Box()
    child1 = Box()
    child2 = Box()

    contains1 = Contains(parent, child1)
    contains2 = Contains(parent, child2)

    storage.save(parent)
    storage.save(child1)
    storage.save(child2)
    storage.save(contains1)
    storage.save(contains2)

    with pytest.raises(MultipleObjectsFound):
        parent.contains.one()


@pytest.mark.usefixtures('storage')
def test_empty_rel_attributes(storage):
    box = Box()

    storage.save(box)
    # TODO: should the attr support len()
    assert len(list(box.contains)) == 0
    assert box.contained_within.first() is None


@pytest.mark.usefixtures('storage')
def test_many_children(storage):

    parent = Box()
    child1 = Box()
    child2 = Box()

    contains1 = Contains(parent, child1)
    contains2 = Contains(parent, child2)

    storage.save(parent)
    storage.save(child1)
    storage.save(child2)
    storage.save(contains1)
    storage.save(contains2)

    assert len(list(parent.contains)) == 2
    assert parent.contains.first().id in [child1.id, child2.id]


@pytest.mark.usefixtures('storage')
def test_reference_relationship_itself(storage):
    parent = Box()
    child = Box()

    contains = Contains(parent, child)

    storage.save(parent)
    storage.save(child)
    storage.save(contains)

    fetched = storage.get(Box, id=str(child.id))
    fetched_rel = next(fetched.contained_within.relationships)

    assert fetched_rel.id == contains.id
