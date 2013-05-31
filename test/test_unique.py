import random
import string

import pytest

from kaiso.exceptions import UniqueConstraintError
from kaiso.types import Entity, Relationship
from kaiso.attributes import Integer, String


class NoIndexThing(Entity):
    field_a = String()


class UniqueThing(Entity):
    id = Integer(unique=True)
    code = String(unique=True)
    extra = String()


class Follows(Relationship):
    id = Integer(unique=True)


class IndexedRel(Relationship):
    id = String(unique=True)


class TestReplace(object):
    def test_unique_enforced_on_add(self, manager):
        """ Currently we can't change unique attributes
            (need to figure out how to retain db integrity during such
            changes)
        """
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        # This is interpreted as find object with id:1 and change
        # obj.code to 'B' (not "try to create a new object (1, 'B') )
        obj2 = UniqueThing(id=1, code='B', extra='snacks')
        with pytest.raises(NotImplementedError):
            manager.save(obj2)

    def test_replace_no_conflict(self, manager):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        manager.save(obj2)

        rows = manager.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)

        assert len(rows) == 2

        assert rows[0][0].id == 1
        assert rows[1][0].id == 2

    def test_no_change(self, manager):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        # should be a no-op, end up with only one obj in db
        obj2 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj2)

        rows = manager.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1

    def test_unique_fail(self, manager):
        obj1 = UniqueThing(id=1, code='A')
        manager.save(obj1)

        obj2 = UniqueThing(id=2, code='C')
        manager.save(obj2)

        # no way to add this thing without breaking a unique constraint
        with pytest.raises(UniqueConstraintError):
            obj3 = UniqueThing(id=1, code='C')
            manager.save(obj3)

    def test_change_non_unique_field(self, manager):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        manager.save(obj1)

        obj2 = UniqueThing(id=1, code='A', extra='ice cream')
        manager.save(obj2)
        rows = manager.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra == 'ice cream'

    def test_remove_prop(self, manager):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        manager.save(obj1)

        obj2 = UniqueThing(id=1, code='A')
        manager.save(obj2)
        rows = manager.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra is None

    def test_no_existing_index(self, manager):
        name = ''.join(random.choice(string.ascii_letters) for _ in range(20))

        # we have no way of removing indexes from the db, so create a new
        # type that we haven't seen before to test the case where
        # the index does not exist
        RandomThing = type(
            name, (Entity,), {'code': String(unique=True)})

        # Register the type manaually
        # Could we create this as a dynamic type and this test still be valid?
        manager.type_registry.register(RandomThing)

        obj = RandomThing(code='a')
        manager.save(obj)

    def test_rel_uniqueness(self, manager):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        manager.save(obj1)
        manager.save(obj2)

        follow_rel1 = Follows(obj1, obj2, id=1)
        manager.save(follow_rel1)

        follow_rel2 = Follows(obj1, obj2, id=1)
        manager.save(follow_rel2)

        result = manager.query('''
            START n = node:uniquething(id="1")
            MATCH n-[r:FOLLOWS]->()
            RETURN r''')

        result = list(result)
        assert len(result) == 1
