import decimal

import iso8601
import pytest
from py2neo import cypher

from kaiso.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)
from kaiso.persistence import TypeSystem
from kaiso.relationships import Relationship
from kaiso.types import PersistableMeta, Entity


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


class MultipleUniques(Entity):
    u1 = String(unique=True)
    u2 = String(unique=True)


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

    result = list(storage.query(
        'START n=node:persistablemeta("id:*") RETURN n')
    )
    assert result == [(Entity,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_entity_once(storage):

    storage.save(Entity)
    storage.save(Entity)

    result = list(storage.query(
        'START n=node:persistablemeta("id:*") RETURN n')
    )
    assert result == [(Entity,)]


@pytest.mark.usefixtures('storage')
def test_only_adds_types_once(storage):
    thing1 = Thing()
    thing2 = Thing()

    storage.save(thing1)
    storage.save(thing2)

    (count,) = next(storage.query(
        'START n=node:persistablemeta(id="Thing") '
        'RETURN count(n)'))

    assert count == 1


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_type(storage):

    storage.save(Thing)

    result = storage.get(PersistableMeta, id='Thing')

    assert result is Thing


@pytest.mark.usefixtures('storage')
def test_get_type_non_existing_obj(storage):
    storage.save(Thing)

    assert storage.get(PersistableMeta, name="Ting") is None


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_instance(storage):
    thing = Thing()
    storage.save(thing)

    queried_thing = storage.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


@pytest.mark.usefixtures('storage')
def test_simple_add_and_get_relationship(storage):
    thing1 = Thing()
    thing2 = Thing()
    rel = IndexedRelated(start=thing1, end=thing2)
    storage.save(thing1)
    storage.save(thing2)
    storage.save(rel)

    queried_rel = storage.get(IndexedRelated, id=rel.id)

    assert type(queried_rel) == IndexedRelated
    assert queried_rel.id == rel.id


@pytest.mark.usefixtures('storage')
def test_delete_instance_types_remain(storage):
    thing = Thing()
    storage.save(thing)

    storage.delete(thing)

    # we are expecting the type to stay in place
    rows = storage.query("""
        START n=node:persistablemeta("id:*")
        MATCH n-[:ISA|INSTANCEOF]->m
        RETURN n""")
    result = set(item for (item,) in rows)
    assert result == {Thing}


@pytest.mark.usefixtures('storage')
def test_delete_relationship(storage):
    """
    Verify that relationships can be removed from the database.

    The nodes that were related should not be removed.
    """
    thing1 = Thing()
    thing2 = Thing()
    rel = Related(thing1, thing2)

    storage.save(thing1)
    storage.save(thing2)
    storage.save(rel)

    storage.delete(rel)

    rows = storage.query("""
        START n1 = node(*)
        MATCH n1 -[r]-> n2
        RETURN n1.id?, r.__type__
    """)

    result = list(rows)
    ids = [item[0] for item in result]
    rels = [item[1] for item in result]

    assert str(thing1.id) in ids
    assert str(thing2.id) in ids
    assert 'Related' not in rels


@pytest.mark.usefixtures('storage')
def test_delete_class(storage):
    """
    Verify that types can be removed from the database.

    The attributes of the type should be removed.
    Instances of the type should not.
    """
    thing = Thing()
    storage.save(thing)

    storage.delete(Thing)
    storage.delete(TypeSystem)

    rows = storage.query('START n=node(*) RETURN COALESCE(n.id?, n)')
    result = set(item for (item,) in rows)
    assert result == {'TypeSystem', 'Entity', str(thing.id)}


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
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """

    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        ('Thing', 'IsA', Entity),
        (str(obj.id), 'InstanceOf', Thing),
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
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """
    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        ('Thing', 'IsA', Entity),
        ('Flavouring', 'IsA', Thing),
        ('Colouring', 'IsA', Thing),
        ('Carmine', 'IsA', Colouring),
        ('Beetroot', 'IsA', Flavouring),
        ('Beetroot', 'IsA', Colouring),
        (str(beetroot.id), 'InstanceOf', Beetroot),
        (str(carmine.id), 'InstanceOf', Carmine)
    }


@pytest.mark.usefixtures('storage')
def test_add_type_creates_index(storage):
    storage.save(Thing)

    # this should not raise a MissingIndex error
    result = list(storage.query('START n=node:thing("id:*") RETURN n'))

    assert result == []


def count(storage, type_):
    type_id = type_.__name__
    query = """
        START Thing=node:persistablemeta(id="{}")
        MATCH (n)-[:INSTANCEOF]->Thing
        RETURN count(n);
        """.format(type_id)
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


@pytest.mark.usefixtures('storage')
def test_persist_attributes(storage):
    """
    Verify persisted attributes maintain their type when added to the
    database.
    """
    storage.save(Thing)

    query_str = """
        START Thing = node:persistablemeta(id="Thing")
        MATCH attr -[DECLAREDON]-> Thing
        RETURN attr
    """

    rows = storage.query(query_str)
    result = set([type(attr[0]) for attr in rows])

    assert result == {
        Uuid,
        Bool,
        Integer,
        Float,
        String,
        Decimal,
        DateTime,
        Choice
    }


@pytest.mark.usefixtures('storage')
def test_attribute_creation(storage):
    """
    Verify that attributes are added to the database when a type is added.
    """
    storage.save(Thing)

    query_str = """
        START Thing = node:persistablemeta(id="Thing")
        MATCH attr -[:DECLAREDON]-> Thing
        RETURN attr.__type__, attr.name, attr.unique
    """

    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        ('Uuid', 'id', True),
        ('Bool', 'bool_attr', False),
        ('Integer', 'int_attr', False),
        ('Float', 'float_attr', False),
        ('String', 'str_attr', False),
        ('Decimal', 'dec_attr', False),
        ('DateTime', 'dt_attr', False),
        ('Choice', 'ch_attr', False),
    }


@pytest.mark.usefixtures('storage')
def test_attribute_inheritance(storage):
    """
    Verify that attributes are created correctly according to type
    inheritence.
    """

    class Flavouring(Entity):
        natural = Bool()
        veggie_friendly = Bool()
        tastes_like = String()

    class Colouring(Entity):
        natural = Bool()
        veggie_friendly = Bool()
        color = String()

    class Beetroot(Flavouring, Colouring):
        natural = Bool(default=True)

    storage.save(Beetroot)

    # ``natural`` on ``Beetroot`` will be found twice by this query, because
    # there are two paths from it to ``Entity``.
    query_str = """
        START Entity = node:persistablemeta(id="Entity")
        MATCH attr -[:DECLAREDON]-> type -[:ISA*]-> Entity
        RETURN type.id, attr.name, attr.__type__, attr.default?
    """

    rows = storage.query(query_str)
    result = set(rows)

    assert result == {
        ('Flavouring', 'natural', 'Bool', None),
        ('Flavouring', 'veggie_friendly', 'Bool', None),
        ('Flavouring', 'tastes_like', 'String', None),
        ('Colouring', 'natural', 'Bool', None),
        ('Colouring', 'veggie_friendly', 'Bool', None),
        ('Colouring', 'color', 'String', None),
        ('Beetroot', 'natural', 'Bool', True),
    }

    # ``natural`` on ``Beetroot`` should only be defined once
    query_str = """
        START Beetroot = node:persistablemeta(id="Beetroot")
        MATCH attr -[:DECLAREDON]-> Beetroot
        RETURN count(attr)
    """
    count = next(storage.query(query_str))[0]
    assert count == 1
