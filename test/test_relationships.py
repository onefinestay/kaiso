import pytest

from orp.attributes import Uuid, Outgoing, Incoming
from orp.relationships import Relationship, many
from orp.types import Persistable


class Contains(Relationship):
    pass


class Box(Persistable):
    id = Uuid(unique=True)

    contains = Outgoing(Contains, 0, many)
    contained_within = Incoming(Contains, 0, 1)


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
    assert box2.contained_within.id == box1.id


def test_empty_rel_attributes(storage):
    box = Box()

    storage.add(box)

    assert len(box.contains) == 0
    assert box.contained_within is None

# TODO: test for creation bug