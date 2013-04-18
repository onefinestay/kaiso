import random
import string

import pytest

from orp.exceptions import UniqueConstraintError, NoIndexesError
from orp.persistence import Storage
from orp.types import PersistableType, Persistable, Relationship
from orp.attributes import Integer, String



class NoIndexThing(Persistable):
    field_a = String()


class UniqueThing(Persistable):
    id = Integer(unique=True)
    code = String(unique=True)
    extra = String()


class Follows(Relationship):
    pass


class IndexedRel(Relationship):
    id = String(unique=True)


class TestReplace(object):
    def test_unique_enforced_on_add(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        storage.add(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=1, code='B', extra='snacks')
        with pytest.raises(UniqueConstraintError):
            storage.add(obj2)

    def test_replace_no_conflict(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        storage.replace(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        storage.replace(obj2)

        rows = storage.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)

        assert len(rows) == 2

        assert rows[0][0].id == 1
        assert rows[1][0].id == 2

    def test_no_change(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        storage.replace(obj1)

        # should be a no-op, end up with only one obj in db
        obj2 = UniqueThing(id=1, code='A', extra='lunch')
        storage.replace(obj2)

        rows = storage.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1

    def test_unique_fail(self, storage):
        obj1 = UniqueThing(id=1, code='A')
        storage.replace(obj1)

        obj2 = UniqueThing(id=2, code='C')
        storage.replace(obj2)

        # no way to add this thing without breaking a unique constraint
        with pytest.raises(UniqueConstraintError):
            obj3 = UniqueThing(id=1, code='C')
            storage.replace(obj3)

    def test_change_non_unique_field(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        storage.replace(obj1)

        obj2 = UniqueThing(id=1, code='A', extra='ice cream')
        storage.replace(obj2)
        rows = storage.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra == 'ice cream'

    def test_remove_prop(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        storage.replace(obj1)

        obj2 = UniqueThing(id=1, code='A')
        storage.replace(obj2)
        rows = storage.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra is None

    def test_cant_replace_non_indexed(self, storage):
        obj1 = NoIndexThing(field='a')
        with pytest.raises(NoIndexesError):
            storage.replace(obj1)

    def test_no_existing_index(self, storage):
        name = ''.join(random.choice(string.ascii_letters) for i in range(20))

        # we have no way of removing indexes from the db, so create a new
        # type that we haven't seen before to test the case where
        # the index does not exist
        RandomThing = PersistableType(
            name, (Persistable,), {'code': String(unique=True)})

        obj = RandomThing(code='a')
        storage.add(obj)

    def test_rel_uniqueness(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        storage.replace(obj1)
        storage.replace(obj2)

        follow_rel1 = Follows(obj1, obj2)
        storage.replace(follow_rel1)

        follow_rel2 = Follows(obj1, obj2)
        storage.replace(follow_rel2)

        result = storage.query('''
            START n = node:uniquething(id="1")
            MATCH n-[r:FOLLOWS]->()
            RETURN r''')

        result = list(result)
        assert len(result) == 1

    def test_indexed_relationships_replace(self, storage):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        storage.replace(obj1)
        storage.replace(obj2)

        with pytest.raises(NotImplementedError):
            storage.replace(IndexedRel(obj1, obj2, id="foo"))
