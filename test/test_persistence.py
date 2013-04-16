import decimal
from datetime import datetime

import pytest

from orp.connection import get_connection
from orp.persistence import Storage
from orp.types import PersistableType, Persistable
from orp.relationships import Relationship
from orp.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)


conn_uri = 'http://localhost:7474/db/data'


def setup_function(fn):
    conn = get_connection(conn_uri)
    conn.clear()


class Thing(Persistable):
    id = Uuid(unique=True)
    bool_attr = Bool()
    int_attr = Integer()
    float_attr = Float()
    str_attr = String()
    dec_attr = Decimal()
    dt_attr = DateTime()
    ch_attr = Choice('a', 'b')


class Related(Relationship):
    str_attr = String()


def test_add_fails_on_no_persistable():
    store = Storage(conn_uri)

    with pytest.raises(TypeError):
        store.add(object())


def test_simple_add_and_get_meta_type():
    store = Storage(conn_uri)

    store.add(PersistableType)
    result = store.get(type, name='PersistableType')
    assert result is PersistableType


def test_simple_add_and_get_type():
    store = Storage(conn_uri)

    store.add(Thing)

    result = store.get(PersistableType, name='Thing')

    assert result is Thing


def test_simple_add_and_get_instance():
    store = Storage(conn_uri)

    thing = Thing()
    store.add(thing)

    queried_thing = store.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


def test_attributes():
    store = Storage(conn_uri)

    thing = Thing(bool_attr=True, init_attr=7)
    thing.float_attr = 3.14
    thing.str_attr = 'spam'
    thing.dec_attr = decimal.Decimal('99.55')
    thing.dt_attr = datetime(2001, 2, 3, 16, 17)
    thing.ch_attr = 'b'

    store.add(thing)

    queried_thing = store.get(Thing, id=thing.id)

    assert queried_thing.id == thing.id
    assert queried_thing.bool_attr == thing.bool_attr
    assert queried_thing.int_attr == thing.int_attr
    assert queried_thing.float_attr == thing.float_attr
    assert queried_thing.str_attr == thing.str_attr
    assert queried_thing.dec_attr == thing.dec_attr
    # assert queried_thing.dt_attr == thing.dt_attr
    assert queried_thing.ch_attr == thing.ch_attr


def test_relationship():
    store = Storage(conn_uri)

    thing1 = Thing()
    thing2 = Thing()

    rel = Related(thing1, thing2, str_attr='5cal')

    store.add(thing1)
    store.add(thing2)
    store.add(rel)

    rows = store.query('''
        START n1 = node:thing(id={id})
        MATCH n1 -[r:RELATED]-> n2
        RETURN n1, r, n2
    ''', id=thing1.id)

    rows = list(rows)
    assert len(rows) == 1

    queried_thing1, queried_rel, queried_thing2 = rows[0]

    assert queried_thing1.id == thing1.id
    assert queried_thing2.id == thing2.id

    assert queried_rel.str_attr == rel.str_attr
    assert queried_rel.start.id == thing1.id
    assert queried_rel.end.id == thing2.id


def test_type_hierarchy_meta():
    store = Storage(conn_uri)

    store.add(PersistableType)

    query_str = """
        START c = node(*)
        RETURN c
    """

    rows = store.query(query_str)
    result = list(rows)
    assert result == [(PersistableType,)]


def test_type_hierarchy_class():
    store = Storage(conn_uri)

    store.add(Persistable)

    query_str = """
        START base = node(*)
        MATCH cls -[r]-> base
        RETURN cls, r.__type__, base
    """

    rows = store.query(query_str)
    result = set(rows)
    assert result == {
        (Persistable, 'InstanceOf', PersistableType)
    }


def test_type_hierarchy_object():
    store = Storage(conn_uri)

    obj = Thing()
    store.add(obj)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """

    rows = store.query(query_str)
    result = set(rows)

    assert result == {
        (Persistable, 'InstanceOf', PersistableType),
        (Thing, 'InstanceOf', PersistableType),
        (Thing, 'IsA', Persistable),
        (str(obj.id), 'InstanceOf', Thing)
    }


def test_type_hierarchy_diamond():
    class Flavouring(Thing):
        pass

    class Colouring(Thing):
        pass

    class Carmine(Colouring):
        pass

    class Beetroot(Flavouring, Colouring):
        pass

    store = Storage(conn_uri)

    beetroot = Beetroot()
    store.add(beetroot)

    carmine = Carmine()
    store.add(carmine)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """
    rows = store.query(query_str)
    result = set(rows)

    assert result == {
        (Persistable, 'InstanceOf', PersistableType),
        (Thing, 'InstanceOf', PersistableType),
        (Thing, 'IsA', Persistable),
        (Flavouring, 'InstanceOf', PersistableType),
        (Flavouring, 'IsA', Thing),
        (Colouring, 'InstanceOf', PersistableType),
        (Colouring, 'IsA', Thing),
        (Carmine, 'InstanceOf', PersistableType),
        (Carmine, 'IsA', Colouring),
        (Beetroot, 'InstanceOf', PersistableType),
        (Beetroot, 'IsA', Flavouring),
        (Beetroot, 'IsA', Colouring),
        (str(beetroot.id), 'InstanceOf', Beetroot),
        (str(carmine.id), 'InstanceOf', Carmine),
    }


def _test_identity():
    store = Storage(conn_uri)
    thing = Thing()
    store.add(thing)

    thing_id = thing.id

    getted_thing = store.get(Thing, id=thing_id)

    rows = store.query('''
        START n = node:thing(id={id})
        RETURN n
    ''', id=thing_id)

    rows = list(rows)

    queried_thing = rows[0][0]

    assert thing is getted_thing
    assert thing is queried_thing

