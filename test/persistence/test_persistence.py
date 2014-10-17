import decimal
from uuid import uuid4

import iso8601
import pytest

from kaiso.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)
from kaiso.exceptions import TypeNotPersistedError
from kaiso.relationships import Relationship, IsA
from kaiso.types import PersistableType, Entity, collector


@pytest.fixture
def beetroot_diamond(request, manager):
    class Thing(Entity):
        id = Uuid(unique=True)
        bool_attr = Bool()
        int_attr = Integer()
        float_attr = Float()
        str_attr = String()
        dec_attr = Decimal()
        dt_attr = DateTime()
        ch_attr = Choice('a', 'b')

    class Flavouring(Thing):
        natural = Bool()
        veggie_friendly = Bool()
        tastes_like = String()

    class Colouring(Thing):
        natural = Bool()
        veggie_friendly = Bool()
        color = String()

    class Beetroot(Flavouring, Colouring):
        natural = Bool(default=True)

    class Carmine(Colouring):
        pass

    class Preservative(Thing):
        e_number = String(unique=True)

    class AnotherThing(Entity):
        name = String()

    manager.save(Thing)

    return {
        'Thing': Thing,
        'Flavouring': Flavouring,
        'Colouring': Colouring,
        'Beetroot': Beetroot,
        'Carmine': Carmine,
        'Preservative': Preservative,
        'AnotherThing': AnotherThing,
    }


@pytest.fixture
def static_types(manager, beetroot_diamond):
    class Related(Relationship):
        str_attr = String()

    result = {
        'Related': Related,
    }
    result.update(beetroot_diamond)
    return result


def test_add_fails_on_non_persistable(manager):

    with pytest.raises(TypeError):
        manager.save(type)

    with pytest.raises(TypeError):
        manager.save(object)

    with pytest.raises(TypeError):
        manager.save(object())

    with pytest.raises(TypeError):
        manager.save(PersistableType)

    # TODO: need to make sure we don't allow adding base classes


def test_add_persistable_only_adds_single_node(manager):

    manager.save(Entity)

    result = list(manager.query(
        'MATCH (n:PersistableType) RETURN n')
    )
    assert result == [(Entity,)]


def test_only_adds_entity_once(manager):
    manager.save(Entity)
    manager.save(Entity)

    result = list(manager.query(
        'MATCH (n:PersistableType) RETURN n')
    )
    assert result == [(Entity,)]


def test_only_adds_types_once(manager, static_types):
    Thing = static_types['Thing']

    thing1 = Thing()
    thing2 = Thing()

    manager.save(thing1)
    manager.save(thing2)

    (count,) = next(manager.query("""
        MATCH (n:PersistableType {id: "Thing"})
        RETURN count(n)
    """))

    assert count == 1


def test_simple_add_and_get_type(manager, static_types):
    Thing = static_types['Thing']

    manager.save(Thing)

    result = manager.get(PersistableType, id='Thing')

    assert result is Thing


def test_get_type_non_existing_obj(manager, static_types):
    Thing = static_types['Thing']

    manager.save(Thing)

    assert manager.get(PersistableType, id="Ting") is None


def test_simple_add_and_get_instance(manager, static_types):
    Thing = static_types['Thing']

    thing = Thing()
    manager.save(thing)

    queried_thing = manager.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


def test_add_and_get_instance_of_node_with_no_attrs(manager):

    # create Thing with no-attrs
    class Thing(Entity):
        pass

    manager.save(Thing)

    thing = Thing()
    manager.save(thing)

    # prove the instance was saved
    rows = manager.query("""
        START n = node(*)
        MATCH (n)-[:INSTANCEOF]->(Thing)
        WHERE Thing.id = "Thing"
        RETURN n
    """)
    result, = next(rows)

    assert isinstance(result, Thing)

    # manager.get will always return None when trying to find
    # a node with no attrs
    queried_thing = manager.get(Thing)
    assert queried_thing is None


def test_add_and_get_instance_of_node_with_no_unique_attrs(manager):

    # create Thing with one non-unique attr
    class Thing(Entity):
        name = String()

    manager.save(Thing)
    thing = Thing(name='foo')
    manager.save(thing)

    # prove the instance was saved
    rows = manager.query("""
        START n = node(*)
        MATCH (n)-[:INSTANCEOF]->(Thing)
        WHERE Thing.id = "Thing"
        RETURN n
    """)
    result, = next(rows)

    assert isinstance(result, Thing)
    assert result.name == 'foo'

    # manager.get will always return None when trying to find
    # a node with no unique attrs
    queried_thing = manager.get(Thing)
    assert queried_thing is None


def test_simple_add_and_get_instance_same_id_different_type(
        manager, static_types):
    """ Instances of two different types that have the same id
    should be distinguishable """

    Thing = static_types['Thing']

    class OtherThing(Entity):
        id = Uuid(unique=True)

    manager.save(OtherThing)

    thing1 = Thing()
    thing2 = OtherThing(id=thing1.id)

    manager.save(thing1)
    manager.save(thing2)

    queried_thing1 = manager.get(Thing, id=thing1.id)
    queried_thing2 = manager.get(OtherThing, id=thing2.id)

    assert type(queried_thing1) == Thing
    assert type(queried_thing2) == OtherThing
    assert queried_thing1.id == queried_thing2.id == thing2.id


def test_simple_add_and_get_instance_by_non_index_attr(manager, static_types):
    Thing = static_types['Thing']

    thing = Thing(str_attr="this is thing")
    manager.save(thing)

    with pytest.raises(ValueError) as exc:
        manager.get(Thing, str_attr=thing.str_attr)
    assert 'No relevant indexes' in str(exc)


def test_simple_add_and_get_instance_with_None_value(manager, static_types):
    Thing = static_types['Thing']

    thing = Thing(id=uuid4())
    manager.save(thing)

    with pytest.raises(ValueError) as exc:
        manager.get(Thing, id=None)
    assert 'No relevant indexes found' in str(exc)


def test_get_with_multi_value_attr_filter(manager, static_types):
    class Thing1(Entity):
        attr_a = Integer(unique=True)
        attr_b = Integer(unique=True)

    class Thing2(Entity):
        attr_a = Integer(unique=True)
        attr_b = Integer(unique=True)

    manager.save(Thing1)
    manager.save(Thing2)

    thing1 = Thing1(attr_a=123, attr_b=999)
    thing2 = Thing2(attr_a=123, attr_b=999)
    manager.save(thing1)
    manager.save(thing2)

    queried_thing = manager.get(Thing1, attr_a=123, attr_b=999)
    assert isinstance(queried_thing, Thing1)
    queried_thing = manager.get(Thing2, attr_a=123, attr_b=999)
    assert isinstance(queried_thing, Thing2)


def test_query_list_values(manager, static_types):
    Related = static_types['Related']

    class ThingA(Entity):
        attr_a = Integer(unique=True)
    manager.save(ThingA)

    thing1 = ThingA(attr_a=1)
    thing2 = ThingA(attr_a=2)

    rel = Related(start=thing1, end=thing2)
    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    query = """
        MATCH (thing1:ThingA)-[rel]->(thing2:ThingA)
        RETURN 1, collect(['foo', thing1, 'bar', thing2, rel])
    """

    rows = list(manager.query(query))
    assert len(rows) == 1
    intval, collection = rows[0]
    assert intval == 1
    assert len(collection) == 1
    data = collection[0]
    assert data[0] == 'foo'
    assert isinstance(data[1], ThingA)
    assert data[1].attr_a == 1
    assert data[2] == 'bar'
    assert isinstance(data[3], ThingA)
    assert data[3].attr_a == 2
    assert isinstance(data[4], Related)


def test_delete_relationship(manager, static_types):
    Thing = static_types['Thing']
    Related = static_types['Related']

    """
    Verify that relationships can be removed from the database.

    The nodes that were related should not be removed.
    """
    thing1 = Thing()
    thing2 = Thing()
    rel = Related(thing1, thing2)

    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    manager.delete(rel)

    rows = manager.query("""
        START n1 = node(*)
        MATCH n1 -[r]-> n2
        RETURN n1.id, r.__type__
    """)

    result = list(rows)
    ids = [item[0] for item in result]
    rels = [item[1] for item in result]

    assert str(thing1.id) in ids
    assert str(thing2.id) in ids
    assert 'Related' not in rels


def test_delete_instance_types_remain(manager):
    class Thing(Entity):
        id = Uuid(unique=True)

    manager.save(Thing)

    thing = Thing()
    manager.save(thing)

    manager.delete(thing)

    # we are expecting the type to stay in place
    rows = manager.query("""
        MATCH (n:PersistableType)
        MATCH (n)-[:ISA|INSTANCEOF]->(m)
        RETURN n
    """)
    result = set(item for (item,) in rows)
    assert result == {Thing}


def test_delete_class(manager):
    """
    Verify that types can be removed from the database.

    The attributes of the type should be removed.
    Instances of the type should not.
    """
    class Thing(Entity):
        id = Uuid(unique=True)

    manager.save(Thing)
    thing = Thing()
    manager.save(thing)

    manager.delete(Thing)

    rows = manager.query('START n=node(*) RETURN COALESCE(n.id, n)')
    result = set(item for (item,) in rows)
    assert result == {'TypeSystem', 'Entity', str(thing.id)}


def test_delete_class_without_attributes(manager):
    """
    Verify that types without attributes can be removed from the database.
    """
    class ParentThing(Entity):
        id = Uuid(unique=True)

    manager.save(ParentThing)

    class Thing(ParentThing):
        pass

    manager.save(Thing)
    thing = Thing()
    manager.save(thing)

    manager.delete(Thing)

    rows = manager.query('START n=node(*) RETURN COALESCE(n.id, n)')
    result = set(item for (item,) in rows)
    assert len(result) == 5
    assert 'Thing' not in result
    assert 'ParentThing' in result
    assert str(thing.id) in result


def test_destroy(manager, static_types):
    Thing = static_types['Thing']

    thing1 = Thing()

    manager.save(thing1)

    # validate test
    assert manager._conn.schema.get_indexed_property_keys('Thing') == ['id']

    manager.destroy()

    count = manager.query_single('MATCH (n) RETURN count(n)')
    assert count == 0

    assert manager._conn.schema.get_indexed_property_keys('Thing') == []


def test_attributes(manager, static_types):
    Thing = static_types['Thing']

    thing = Thing(bool_attr=True, init_attr=7)
    thing.float_attr = 3.14
    thing.str_attr = 'spam'
    thing.dec_attr = decimal.Decimal('99.55')
    thing.dt_attr = iso8601.parse_date("2001-02-03 16:17:00")
    thing.ch_attr = 'b'

    manager.save(thing)

    queried_thing = manager.get(Thing, id=thing.id)

    assert queried_thing.id == thing.id
    assert queried_thing.bool_attr == thing.bool_attr
    assert queried_thing.int_attr == thing.int_attr
    assert queried_thing.float_attr == thing.float_attr
    assert queried_thing.str_attr == thing.str_attr
    assert queried_thing.dec_attr == thing.dec_attr
    assert queried_thing.dt_attr == thing.dt_attr
    assert queried_thing.ch_attr == thing.ch_attr


def test_relationship(manager, static_types):
    Thing = static_types['Thing']
    Related = static_types['Related']

    thing1 = Thing()
    thing2 = Thing()

    rel = Related(thing1, thing2, str_attr='5cal')

    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    rows = manager.query('''
        MATCH (n1:Thing {id: {id}})-[r:RELATED]->(n2)
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


def test_get_type_hierarchy(manager):
    class Thing(Entity):
        id = Uuid(unique=True)

    class Colouring(Thing):
        pass

    class Carmine(Colouring):
        pass

    manager.save(Carmine)  # also saves all parents

    obj1 = Thing()
    obj2 = Colouring()
    obj3 = Carmine()

    manager.save(obj1)
    manager.save(obj2)
    manager.save(obj3)

    hierarchy1 = manager.get_type_hierarchy()
    entities = [e[0] for e in hierarchy1]

    assert len(entities) == 4
    assert entities[0] == Entity.__name__
    assert entities[1] == Thing.__name__
    assert entities[2] == Colouring.__name__
    assert entities[3] == Carmine.__name__

    hierarchy2 = manager.get_type_hierarchy(
        start_type_id='Colouring'
    )
    entities = [e[0] for e in hierarchy2]

    assert len(entities) == 2
    assert entities[0] == Colouring.__name__
    assert entities[1] == Carmine.__name__


def test_get_type_hierarchy_bases_order(manager, beetroot_diamond):
    Beetroot = beetroot_diamond['Beetroot']

    # before we introduced 'base_index' on IsA, this test would fail because
    # we removed and re-added one of the IsA relationships, which
    # caused the types to be loaded in the incorrect order
    manager.save(Beetroot)

    is_a_props = manager.type_registry.object_to_dict(IsA())
    is_a_props['base_index'] = 1

    list(manager.query("""
        MATCH
            (Beetroot:PersistableType {id: "Beetroot"})-[r:ISA]->
                (Colouring:PersistableType {id: "Colouring"})
        DELETE r
        CREATE
            (Beetroot)-[nr:ISA {is_a_props}]->(Colouring)
        RETURN nr
    """, is_a_props=is_a_props))

    result = [(nme, bases) for (nme, bases, _)
              in manager.get_type_hierarchy()]

    assert set(result) == set((
        ('Entity', tuple()),
        ('Thing', ('Entity',)),
        ('Colouring', ('Thing',)),
        ('Flavouring', ('Thing',)),
        ('Beetroot', ('Flavouring', 'Colouring')),
    ))


def test_type_hierarchy_object(manager):
    class Thing(Entity):
        id = Uuid(unique=True)

    manager.save(Thing)
    obj = Thing()
    manager.save(obj)

    query_str = """
        START base = node(*)
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id, obj) , r.__type__, base
    """

    rows = manager.query(query_str)
    result = set(rows)

    assert result == {
        ('Thing', 'IsA', Entity),
        (str(obj.id), 'InstanceOf', Thing),
    }


def test_type_hierarchy_diamond(manager, beetroot_diamond):
    Thing = beetroot_diamond['Thing']
    Colouring = beetroot_diamond['Colouring']
    Flavouring = beetroot_diamond['Flavouring']
    Beetroot = beetroot_diamond['Beetroot']
    Carmine = beetroot_diamond['Carmine']

    manager.save(Beetroot)
    manager.save(Carmine)

    beetroot = Beetroot()
    carmine = Carmine()

    manager.save(carmine)
    manager.save(beetroot)

    query_str = """
        START base = node(*)
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id, obj) , r.__type__, base
    """
    rows = manager.query(query_str)
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


def test_add_type_creates_index(manager, static_types):
    Thing = static_types['Thing']

    # Thing has a unique attr so should create an index
    manager.save(Thing)
    assert manager._conn.schema.get_indexed_property_keys('Thing') == ['id']


def test_add_type_only_creates_indexes_for_unique_attrs(manager, static_types):
    Flavouring = static_types['Flavouring']

    manager.save(Flavouring)

    # superclass Thing has a unique attr so should create an index
    assert manager._conn.schema.get_indexed_property_keys('Thing') == ['id']

    # but Flavouring has no unique attr so should not create an index
    assert manager.query_single('MATCH (n:Flavouring) RETURN n') is None


def test_add_type_with_no_unique_attrs(manager, static_types):
    AnotherThing = static_types['AnotherThing']

    manager.save(AnotherThing)
    assert manager._conn.schema.get_indexed_property_keys('AnotherThing') == []

    # create an instance
    AnotherThing(name='Foo')
    assert manager.query_single('MATCH (n:AnotherThing) RETURN n') is None


def count(manager, type_):
    type_id = type_.__name__
    query = """
        MATCH
            (Thing:PersistableType {id: {type_id}}),
            (n)-[:INSTANCEOF]->(Thing)
        RETURN
            count(n);
        """
    rows = manager.query(query, type_id=type_id)
    (count,) = next(rows)
    return count


def test_save(manager, static_types):
    Thing = static_types['Thing']

    obj = Thing()
    manager.save(obj)
    assert count(manager, Thing) == 1


def test_save_unknown_class(manager):
    class Thing(Entity):
        pass

    thing = Thing()
    with pytest.raises(TypeNotPersistedError):
        manager.save(thing)


def test_save_new(manager, static_types):
    Thing = static_types['Thing']

    obj1 = Thing()
    obj2 = Thing()
    manager.save(obj1)
    manager.save(obj2)
    assert count(manager, Thing) == 2


def test_save_replace(manager, static_types):
    Thing = static_types['Thing']

    obj1 = Thing()
    obj2 = Thing()

    obj2.id = obj1.id
    manager.save(obj1)
    manager.save(obj2)
    assert count(manager, Thing) == 1


def test_save_update(manager, static_types):
    Thing = static_types['Thing']

    obj = Thing(str_attr='one')

    manager.save(obj)

    obj.str_attr = 'two'
    manager.save(obj)

    retrieved = manager.get(Thing, id=obj.id)
    assert retrieved.str_attr == 'two'


def test_persist_attributes(manager):
    """
    Verify persisted attributes maintain their type when added to the
    database.
    """
    class Thing(Entity):
        id = Uuid(unique=True)
        bool_attr = Bool()
        int_attr = Integer()
        float_attr = Float()
        str_attr = String()
        dec_attr = Decimal()
        dt_attr = DateTime()
        ch_attr = Choice('a', 'b')

    manager.save(Thing)

    query_str = """
        MATCH
            (Thing:PersistableType {id: "Thing"}),
            (attr)-[DECLAREDON]->Thing
        RETURN
            attr
    """

    rows = manager.query(query_str)
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


def test_attribute_creation(manager, static_types):
    """
    Verify that attributes are added to the database when a type is added.
    """
    Thing = static_types['Thing']
    manager.save(Thing)

    query_str = """
        MATCH
            (Thing:PersistableType {id: "Thing"}),
            (attr)-[:DECLAREDON]->(Thing)
        RETURN
            attr.__type__, attr.name, attr.unique
    """

    rows = manager.query(query_str)
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


def test_attribute_inheritance(manager, beetroot_diamond):
    """
    Verify that attributes are created correctly according to type
    inheritence.
    """

    Beetroot = beetroot_diamond['Beetroot']
    Carmine = beetroot_diamond['Carmine']

    manager.save(Beetroot)
    manager.save(Carmine)

    # ``natural`` on ``Beetroot`` will be found twice by this query, because
    # there are two paths from it to ``Entity``.
    query_str = """
        MATCH
            (Entity:PersistableType {id: "Entity"}),
            (attr)-[:DECLAREDON]->(type)-[:ISA*]->(Entity)
        RETURN
            type.id, attr.name, attr.__type__, attr.default
    """

    rows = manager.query(query_str)
    result = set(rows)

    assert result == {
        ('Thing', 'str_attr', 'String', None),
        ('Thing', 'ch_attr', 'Choice', None),
        ('Thing', 'dt_attr', 'DateTime', None),
        ('Thing', 'int_attr', 'Integer', None),
        ('Thing', 'id', 'Uuid', None),
        ('Thing', 'bool_attr', 'Bool', None),
        ('Thing', 'dec_attr', 'Decimal', None),
        ('Thing', 'float_attr', 'Float', None),
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
        MATCH
            (Beetroot:PersistableType {id: "Beetroot"}),
            (attr)-[:DECLAREDON]->(Beetroot)
        RETURN
            count(attr)
    """
    count = next(manager.query(query_str))[0]
    assert count == 1


def test_serialize_deserialize(manager):
    """
    Verify that serialize and deserialize are reversible
    """
    dct = manager.serialize(Entity)
    assert dct == {'__type__': 'PersistableType', 'id': 'Entity'}

    obj = manager.deserialize(dct)
    assert obj is Entity


def test_changing_bases_does_not_create_duplicate_types(manager):
    with collector() as classes:
        class ShrubBaseA(Entity):
            id = Uuid()

        class ShrubBaseB(Entity):
            pass

        class Shrub(ShrubBaseA):
            pass

    manager.save_collected_classes(classes)
    del Shrub

    manager.reload_types()

    with collector() as classes:
        class Shrub(ShrubBaseB, ShrubBaseA):
            pass

        class SubShrub(Shrub):
            pass

    manager.type_registry.register(SubShrub)
    manager.save(SubShrub)

    rows = manager.query(
        ''' START base = node(*)
            MATCH tpe -[r:ISA]-> base
            RETURN tpe.id , r.__type__, r.base_index, base.id
            ORDER BY tpe.id, r.base_index, base.id
        ''')
    result = list(rows)

    assert result == [
        ('Shrub', 'IsA', 0, 'ShrubBaseB'),
        ('Shrub', 'IsA', 1, 'ShrubBaseA'),
        ('ShrubBaseA', 'IsA', 0, 'Entity'),
        ('ShrubBaseB', 'IsA', 0, 'Entity'),
        ('SubShrub', 'IsA', 0, 'Shrub'),
    ]
