import pytest

from py2neo.exceptions import CypherError

from kaiso.attributes import Integer, String
from kaiso.types import Entity, Relationship


@pytest.fixture
def static_types(manager):
    class UniqueThing(Entity):
        id = Integer(unique=True)
        code = String(unique=True)
        extra = String()

    manager.save(UniqueThing)

    return {
        'UniqueThing': UniqueThing,
    }


class TestReplace(object):
    def test_conflicting_uniqies(self, manager, static_types):
        UniqueThing = static_types['UniqueThing']
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        obj2 = UniqueThing(id=1, code='B', extra='snacks')

        # this will not find obj1 (code differs), so will try to create
        # a new object, raising an integrity error in the db
        with pytest.raises(CypherError) as exc:
            manager.save(obj2)
        msg = 'already exists with label UniqueThing and property "id"=[1]'
        assert msg in str(exc)

    def test_replace_no_conflict(self, manager, static_types):
        UniqueThing = static_types['UniqueThing']

        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        manager.save(obj2)

        rows = manager.query("MATCH (n:UniqueThing) RETURN n")
        rows = list(rows)

        assert len(rows) == 2

        assert rows[0][0].id == 1
        assert rows[1][0].id == 2

    def test_no_change(self, manager, static_types):
        UniqueThing = static_types['UniqueThing']

        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj1)

        # should be a no-op, end up with only one obj in db
        obj2 = UniqueThing(id=1, code='A', extra='lunch')
        manager.save(obj2)

        rows = manager.query("MATCH (n:UniqueThing) RETURN n")
        rows = list(rows)
        assert len(rows) == 1

    def test_change_non_unique_field(self, manager, static_types):
        UniqueThing = static_types['UniqueThing']

        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        manager.save(obj1)

        obj2 = UniqueThing(id=1, code='A', extra='ice cream')
        manager.save(obj2)
        rows = manager.query("MATCH (n:UniqueThing) RETURN n")
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra == 'ice cream'

    def test_remove_prop(self, manager, static_types):
        UniqueThing = static_types['UniqueThing']

        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        manager.save(obj1)

        obj2 = UniqueThing(id=1, code='A')
        manager.save(obj2)
        rows = manager.query("MATCH (n:UniqueThing) RETURN n")
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra is None

    def test_rel_uniqueness(
            self, manager, static_types):
        UniqueThing = static_types['UniqueThing']

        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        manager.save(obj1)
        manager.save(obj2)

        class Follows(Relationship):
            value = Integer()

        follow_rel1 = Follows(obj1, obj2, value=1)
        manager.save(follow_rel1)

        follow_rel2 = Follows(obj1, obj2, value=1)
        manager.save(follow_rel2)

        result = manager.query("""
            MATCH
                (n:UniqueThing {id: 1}),
                (n)-[r:FOLLOWS]->()
            RETURN
                r
        """)

        result = list(result)
        assert len(result) == 1
