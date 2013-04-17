import decimal
from datetime import datetime

import pytest

from orp.connection import get_connection
from orp.exceptions import UniqueConstraintError
from orp.persistence import Storage
from orp.types import PersistableType, Persistable
from orp.relationships import Relationship
from orp.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)


conn_uri = 'http://localhost:7474/db/data'


class UniqueThing(Persistable):
    id = Integer(unique=True)
    code = String(unique=True)
    extra = String()


class TestReplace(object):

    def setup_method(self, method):
        conn = get_connection(conn_uri)
        conn.clear()
        self.store = Storage(conn_uri)

    def test_unique_enforced_on_add(self):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        self.store.add(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=1, code='B', extra='snacks')
        with pytest.raises(UniqueConstraintError):
            self.store.add(obj2)

    def test_replace_no_conflict(self):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        self.store.replace(obj1)

        # should add as no unique clash
        obj2 = UniqueThing(id=2, code='B', extra='snacks')
        self.store.replace(obj2)

        rows = self.store.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)

        assert len(rows) == 2

        assert rows[0][0].id == 1
        assert rows[1][0].id == 2

    def test_no_change(self):
        obj1 = UniqueThing(id=1, code='A', extra='lunch')
        self.store.replace(obj1)

        # should be a no-op, end up with only one obj in db
        obj2 = UniqueThing(id=1, code='A', extra='lunch')
        self.store.replace(obj2)

        rows = self.store.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1

    def test_unique_fail(self):
        obj1 = UniqueThing(id=1, code='A')
        self.store.replace(obj1)

        obj2 = UniqueThing(id=2, code='C')
        self.store.replace(obj2)

        # no way to add this thing without breaking a unique constraint
        with pytest.raises(UniqueConstraintError):
            obj3 = UniqueThing(id=1, code='C')
            self.store.replace(obj3)

    def test_change_non_unique_field(self):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        self.store.replace(obj1)

        obj2 = UniqueThing(id=1, code='A', extra='ice cream')
        self.store.replace(obj2)
        rows = self.store.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows = list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra == 'ice cream'

    def test_remove_prop(self):
        obj1 = UniqueThing(id=1, code='A', extra='chocolate')
        self.store.replace(obj1)

        obj2 = UniqueThing(id=1, code='A')
        self.store.replace(obj2)
        rows = self.store.query('''START n = node:uniquething("id:*")
                                   RETURN n''')
        rows= list(rows)
        assert len(rows) == 1
        assert rows[0][0].extra is None

