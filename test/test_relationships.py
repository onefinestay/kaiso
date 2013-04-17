import pytest

from orp.attributes import Uuid, Incoming, Outgoing
from orp.exceptions import MultipleObjectsFound, NoResultFound
from orp.relationships import Relationship
from orp.types import Persistable


class Contains(Relationship):
    pass


class Box(Persistable):
    id = Uuid(unique=True)

    contains = Outgoing(Contains)
    contained_within = Incoming(Contains)


@pytest.mark.usefixtures('storage')
def test_rel_attributes(storage):
    box1 = Box()
    box2 = Box()
    contains = Contains(box1, box2)

    # box1.contained_within = box1
    storage.add(box1)
    storage.add(box2)
    storage.add(contains)

    assert [b.id for b in box1.contains] == [box2.id]
    assert box2.contained_within.one().id == box1.id


@pytest.mark.usefixtures('storage')
def test_rel_one_missing(storage):
    box = Box()

    storage.add(box)

    with pytest.raises(NoResultFound):
        box.contains.one()


@pytest.mark.usefixtures('storage')
def test_rel_one_multiple(storage):
    parent = Box()
    child1 = Box()
    child2 = Box()

    contains1 = Contains(parent, child1)
    contains2 = Contains(parent, child2)

    storage.add(parent)
    storage.add(child1)
    storage.add(child2)
    storage.add(contains1)
    storage.add(contains2)

    with pytest.raises(MultipleObjectsFound):
        parent.contains.one()


@pytest.mark.usefixtures('storage')
def test_empty_rel_attributes(storage):
    box = Box()

    storage.add(box)
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

    storage.add(parent)
    storage.add(child1)
    storage.add(child2)
    storage.add(contains1)
    storage.add(contains2)

    assert len(list(parent.contains)) == 2
    assert parent.contains.first().id in [child1.id, child2.id]
