import decimal

import iso8601
import pytest
from py2neo import cypher

from kaiso.types import PersistableType, Persistable
from kaiso.relationships import Relationship
from kaiso.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)


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


class IndexedRelated(Relationship):
    id = Uuid(unique=True)


@pytest.mark.usefixtures('storage')
def test_add_fails_on_non_persistable(storage):

    with pytest.raises(TypeError):
        storage.add(object())

    with pytest.raises(TypeError):
        storage.add(PersistableType)

    with pytest.raises(TypeError):
        storage.add(Relationship)

    with pytest.raises(TypeError):
        storage.add(Related)


@pytest.mark.usefixtures('storage')
def test_add_persistable_only_adds_single_node(storage):

    storage.add(Persistable)

    result = list(storage.query('START n=node(*) RETURN n'))
    assert result == [(Persistable,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_persistable_once(storage):

    storage.add(Persistable)
    storage.add(Persistable)

    result = list(storage.query('START n=node(*) RETURN n'))
    assert result == [(Persistable,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_types_once(storage):
    thing1 = Thing()
    thing2 = Thing()

    storage.add(thing1)
    storage.add(thing2)

    rows = storage.query('START n=node(*) RETURN COALESCE(n.id?, n)')

    result = set(item for (item,) in rows)

    assert result == {Persistable, Thing, str(thing1.id), str(thing2.id)}


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_type(storage):

    storage.add(Thing)

    result = storage.get(PersistableType, name='Thing')

    assert result is Thing


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_instance(storage):
    thing = Thing()
    storage.add(thing)

    queried_thing = storage.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


@pytest.mark.usefixtures('storage')
def test_delete_instance(storage):
    thing = Thing()
    storage.add(thing)

    storage.delete(thing)

    # we are expecting the types to stay in place
    rows = storage.query('START n=node(*) RETURN n')
    result = set(item for (item,) in rows)
    assert result == {Persistable, Thing}


@pytest.mark.usefixtures('storage')
def test_delete_relationship(storage):
    thing1 = Thing()
    thing2 = Thing()
    rel = Related(thing1, thing2)

    storage.add(thing1)
    storage.add(thing2)
    storage.add(rel)

    storage.delete(rel)

    rows = storage.query('''
        START n1 = node(*)
        MATCH n1 -[r]-> n2
        RETURN COALESCE(n1.id?, n1), r.__type__, n2
    ''')

    result = set(rows)

    assert result == {
        (Thing, 'IsA', Persistable),
        (str(thing1.id), 'InstanceOf', Thing),
        (str(thing2.id), 'InstanceOf', Thing),
    }


@pytest.mark.usefixtures('storage')
def test_delete_class(storage):
    thing = Thing()
    storage.add(thing)

    storage.delete(Thing)

    # we are expecting the instances to stay in place
    rows = storage.query('START n=node(*) RETURN COALESCE(n.id?, n)')
    result = set(item for (item,) in rows)

    assert result == {Persistable, str(thing.id)}


@pytest.mark.usefixtures('storage')
def test_delete_all_data(storage):

    thing1 = Thing()
    thing2 = Thing()

    storage.add(thing1)
    storage.add(thing2)
    storage.add(IndexedRelated(thing1, thing2))

    storage.delete_all_data()

    rows = storage.query('START n=node(*) RETURN count(n)')
    assert next(rows) == (0,)

    queries = (
        'START n=node:persistableype(name="Thing") RETURN n',
        'START r=relationship:indexedrelated(id="spam") RETURN r',
    )

    for query in queries:
        rows = storage.query(query)

        with pytest.raises(cypher.CypherError) as excinfo:
            next(rows)

        assert excinfo.value.exception == 'MissingIndexException'


@pytest.mark.usefixtures('storage')
def test_attributes(storage):

    thing = Thing(bool_attr=True, init_attr=7)
    thing.float_attr = 3.14
    thing.str_attr = 'spam'
    thing.dec_attr = decimal.Decimal('99.55')
    thing.dt_attr = iso8601.parse_date("2001-02-03 16:17:00")
    thing.ch_attr = 'b'

    storage.add(thing)

    queried_thing = storage.get(Thing, id=thing.id)

    assert queried_thing.id == thing.id
    assert queried_thing.bool_attr == thing.bool_attr
    assert queried_thing.int_attr == thing.int_attr
    assert queried_thing.float_attr == thing.float_attr
    assert queried_thing.str_attr == thing.str_attr
    assert queried_thing.dec_attr == thing.dec_attr
    assert queried_thing.dt_attr == thing.dt_attr
    assert queried_thing.ch_attr == thing.ch_attr


@pytest.mark.usefixtures('storage')
def test_relationship(storage):
    thing1 = Thing()
    thing2 = Thing()

    rel = Related(thing1, thing2, str_attr='5cal')

    storage.add(thing1)
    storage.add(thing2)
    storage.add(rel)

    rows = storage.query('''
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


@pytest.mark.usefixtures('storage')
def test_indexed_relationship(storage):
    thing1 = Thing()
    thing2 = Thing()

    rel = IndexedRelated(thing1, thing2)

    storage.add(thing1)
    storage.add(thing2)
    storage.add(rel)

    rows = storage.query('''
        START r = relationship:indexedrelated(id={rel_id})
        MATCH n1 -[r]-> n2
        RETURN n1.id, n2.id
    ''', rel_id=rel.id)

    result = set(rows)

    assert result == {
        (str(thing1.id), str(thing2.id))
    }


@pytest.mark.usefixtures('storage')
def test_type_hierarchy_object(storage):
    obj = Thing()
    storage.add(obj)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """

    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        (Thing, 'IsA', Persistable),
        (str(obj.id), 'InstanceOf', Thing)
    }


@pytest.mark.usefixtures('storage')
def test_type_hierarchy_diamond(storage):
    class Flavouring(Thing):
        pass

    class Colouring(Thing):
        pass

    class Carmine(Colouring):
        pass

    class Beetroot(Flavouring, Colouring):
        pass

    beetroot = Beetroot()
    storage.add(beetroot)

    carmine = Carmine()
    storage.add(carmine)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """
    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        (Thing, 'IsA', Persistable),
        (Flavouring, 'IsA', Thing),
        (Colouring, 'IsA', Thing),
        (Carmine, 'IsA', Colouring),
        (Beetroot, 'IsA', Flavouring),
        (Beetroot, 'IsA', Colouring),
        (str(beetroot.id), 'InstanceOf', Beetroot),
        (str(carmine.id), 'InstanceOf', Carmine),
    }
