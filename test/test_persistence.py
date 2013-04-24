import decimal

import iso8601
import pytest
from py2neo import cypher

from kaiso.types import PersistableMeta, Entity
from kaiso.relationships import Relationship
from kaiso.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)


class Thing(Entity):
    id = Uuid(unique=True)
    bool_attr = Bool()
    int_attr = Integer()
    float_attr = Float()
    str_attr = String()
    dec_attr = Decimal()
    dt_attr = DateTime()
    ch_attr = Choice('a', 'b')


class NonUnique(Entity):
    val = String()


class Related(Relationship):
    str_attr = String()


class IndexedRelated(Relationship):
    id = Uuid(unique=True)


@pytest.mark.usefixtures('storage')
def test_add_fails_on_non_persistable(storage):

    with pytest.raises(TypeError):
        storage.save(type)

    with pytest.raises(TypeError):
        storage.save(object)

    with pytest.raises(TypeError):
        storage.save(object())

    with pytest.raises(TypeError):
        storage.save(PersistableMeta)

    # TODO: need to make sure we don't allow adding base classes


@pytest.mark.usefixtures('storage')
def test_add_persistable_only_adds_single_node(storage):

    storage.save(Entity)

    result = list(storage.query('START n=node(*) RETURN n'))
    assert result == [(Entity,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_entity_once(storage):

    storage._add(Entity)
    storage._add(Entity)

    result = list(storage.query('START n=node(*) RETURN n'))
    assert result == [(Entity,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_types_once(storage):
    thing1 = Thing()
    thing2 = Thing()

    storage.save(thing1)
    storage.save(thing2)

    rows = storage.query('START n=node(*) RETURN COALESCE(n.id?, n)')

    result = set(item for (item,) in rows)

    assert result == {Entity, Thing, str(thing1.id), str(thing2.id)}


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_type(storage):

    storage.save(Thing)

    result = storage.get(PersistableMeta, name='Thing')

    assert result is Thing


@pytest.mark.usefixtures('storage')
def test_get_type_non_existing_index(storage):

    thing = Thing()
    storage.save(thing)

    with pytest.raises(AttributeError):
        storage.get(Thing, foobar='Thing')


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_instance(storage):
    thing = Thing()
    storage.save(thing)

    queried_thing = storage.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


@pytest.mark.usefixtures('storage')
def test_delete_instance(storage):
    thing = Thing()
    storage.save(thing)

    storage.delete(thing)

    # we are expecting the types to stay in place
    rows = storage.query('START n=node(*) RETURN n')
    result = set(item for (item,) in rows)
    assert result == {Entity, Thing}


@pytest.mark.usefixtures('storage')
def test_delete_relationship(storage):
    thing1 = Thing()
    thing2 = Thing()
    rel = Related(thing1, thing2)

    storage.save(thing1)
    storage.save(thing2)
    storage.save(rel)

    storage.delete(rel)

    rows = storage.query('''
        START n1 = node(*)
        MATCH n1 -[r]-> n2
        RETURN COALESCE(n1.id?, n1), r.__type__, n2
    ''')

    result = set(rows)

    assert result == {
        (Thing, 'IsA', Entity),
        (str(thing1.id), 'InstanceOf', Thing),
        (str(thing2.id), 'InstanceOf', Thing),
    }


@pytest.mark.usefixtures('storage')
def test_delete_class(storage):
    thing = Thing()
    storage.save(thing)

    storage.delete(Thing)

    # we are expecting the instances to stay in place
    rows = storage.query('START n=node(*) RETURN COALESCE(n.id?, n)')
    result = set(item for (item,) in rows)

    assert result == {Entity, str(thing.id)}


@pytest.mark.usefixtures('storage')
def test_delete_all_data(storage):

    thing1 = Thing()
    thing2 = Thing()

    storage.save(thing1)
    storage.save(thing2)
    storage.save(IndexedRelated(thing1, thing2))

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

    storage.save(thing)

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

    storage.save(thing1)
    storage.save(thing2)
    storage.save(rel)

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

    storage.save(thing1)
    storage.save(thing2)
    storage.save(rel)

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
    storage.save(obj)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """

    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        (Thing, 'IsA', Entity),
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
    storage.save(beetroot)

    carmine = Carmine()
    storage.save(carmine)

    query_str = """
        START base = node(*)
        MATCH obj -[r]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """
    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        (Thing, 'IsA', Entity),
        (Flavouring, 'IsA', Thing),
        (Colouring, 'IsA', Thing),
        (Carmine, 'IsA', Colouring),
        (Beetroot, 'IsA', Flavouring),
        (Beetroot, 'IsA', Colouring),
        (str(beetroot.id), 'InstanceOf', Beetroot),
        (str(carmine.id), 'InstanceOf', Carmine),
    }


def count(storage, type_):
    type_name = type_.__name__
    query = """
        START Thing=node:persistablemeta(name="{}")
        MATCH (n)-[:INSTANCEOF]->Thing
        RETURN count(n);
        """.format(type_name)
    rows = storage.query(query)
    (count,) = next(rows)
    return count


@pytest.mark.usefixtures('storage')
def test_save(storage):
    obj = Thing()
    storage.save(obj)
    assert count(storage, Thing) == 1


@pytest.mark.usefixtures('storage')
def test_save_new(storage):
    obj1 = Thing()
    obj2 = Thing()
    storage.save(obj1)
    storage.save(obj2)
    assert count(storage, Thing) == 2


@pytest.mark.usefixtures('storage')
def test_save_replace(storage):
    obj1 = Thing()
    obj2 = Thing()

    obj2.id = obj1.id
    storage.save(obj1)
    storage.save(obj2)
    assert count(storage, Thing) == 1


@pytest.mark.usefixtures('storage')
def test_save_update(storage):
    obj = Thing(str_attr='one')

    storage.save(obj)

    obj.str_attr = 'two'
    storage.save(obj)

    retrieved = storage.get(Thing, id=obj.id)
    assert retrieved.str_attr == 'two'
