import decimal
import uuid

import iso8601
from mock import patch
import pytest
from py2neo import cypher

from kaiso.attributes import (
    Uuid, Bool, Integer, Float, String, Decimal, DateTime, Choice)
from kaiso.persistence import TypeSystem
from kaiso.queries import get_start_clause, join_lines
from kaiso.relationships import Relationship, IsA
from kaiso.types import PersistableType, Entity, collector, get_type_id


class Thing(Entity):
    id = Uuid(unique=True)
    bool_attr = Bool()
    int_attr = Integer()
    float_attr = Float()
    str_attr = String()
    dec_attr = Decimal()
    dt_attr = DateTime()
    ch_attr = Choice('a', 'b')


class OtherThing(Entity):
    id = Uuid(unique=True)


class ClassAttrThing(Entity):
    id = Uuid(unique=True)
    cls_attr = "spam"


class NonUnique(Entity):
    val = String()


class MultipleUniques(Entity):
    u1 = String(unique=True)
    u2 = String(unique=True)


class Related(Relationship):
    str_attr = String()


class IndexedRelated(Relationship):
    id = Uuid(unique=True)


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
        'START n=node:persistabletype("id:*") RETURN n')
    )
    assert result == [(Entity,)]


def test_only_adds_entity_once(manager):

    manager.save(Entity)
    manager.save(Entity)

    result = list(manager.query(
        'START n=node:persistabletype("id:*") RETURN n')
    )
    assert result == [(Entity,)]


def test_only_adds_types_once(manager):
    thing1 = Thing()
    thing2 = Thing()

    manager.save(thing1)
    manager.save(thing2)

    (count,) = next(manager.query(
        'START n=node:persistabletype(id="Thing") '
        'RETURN count(n)'))

    assert count == 1


def test_simple_add_and_get_type(manager):
    manager.save(Thing)

    result = manager.get(PersistableType, id='Thing')

    assert result is Thing


def test_get_type_non_existing_obj(manager):
    manager.save(Thing)

    assert manager.get(PersistableType, name="Ting") is None


def test_simple_add_and_get_instance(manager):
    thing = Thing()
    manager.save(thing)

    queried_thing = manager.get(Thing, id=thing.id)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing.id


def test_simple_add_and_get_instance_same_id_different_type(manager):
    """ Instances of two different types that have the same id
    should be distinguishable """

    thing1 = Thing()
    thing2 = OtherThing(id=thing1.id)

    manager.save(thing1)
    manager.save(thing2)

    queried_thing1 = manager.get(Thing, id=thing1.id)
    queried_thing2 = manager.get(OtherThing, id=thing2.id)

    assert type(queried_thing1) == Thing
    assert type(queried_thing2) == OtherThing
    assert queried_thing1.id == queried_thing2.id == thing2.id


def test_simple_add_and_get_instance_by_optional_attr(manager):
    thing1 = Thing()
    thing2 = Thing(str_attr="this is thing2")
    manager.save(thing1)
    manager.save(thing2)

    queried_thing = manager.get(Thing, str_attr=thing2.str_attr)

    assert type(queried_thing) == Thing
    assert queried_thing.id == thing2.id
    assert queried_thing.str_attr == thing2.str_attr


def test_simple_add_and_get_relationship(manager):
    thing1 = Thing()
    thing2 = Thing()
    rel = IndexedRelated(start=thing1, end=thing2)
    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    queried_rel = manager.get(IndexedRelated, id=rel.id)

    assert type(queried_rel) == IndexedRelated
    assert queried_rel.id == rel.id
    assert queried_rel.start.id == thing1.id
    assert queried_rel.end.id == thing2.id


def test_delete_relationship(manager):
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
        RETURN n1.id?, r.__type__
    """)

    result = list(rows)
    ids = [item[0] for item in result]
    rels = [item[1] for item in result]

    assert str(thing1.id) in ids
    assert str(thing2.id) in ids
    assert 'Related' not in rels


def test_delete_indexed_relationship(manager):
    """ Verify that indexed relationships can be deleted from the database
    without needing references to the start and end nodes.
    """
    thing1 = Thing()
    thing2 = Thing()
    rel = IndexedRelated(thing1, thing2)

    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    # serialize and deserialize to remove references
    rel = manager.deserialize(manager.serialize(rel))
    assert not hasattr(rel, "start")

    manager.delete(rel)

    rows = manager.query("""
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


def test_update_relationship_end_points(manager):
    thing1 = Thing()
    thing2 = Thing()
    thing3 = Thing()

    manager.save(thing1)
    manager.save(thing2)
    manager.save(thing3)

    rel = IndexedRelated(start=thing1, end=thing2)
    manager.save(rel)

    rel.end = thing3
    manager.save(rel)
    queried_rel = manager.get(IndexedRelated, id=rel.id)
    assert queried_rel.start.id == thing1.id
    assert queried_rel.end.id == thing3.id

    rel.start = thing2
    manager.save(rel)
    queried_rel = manager.get(IndexedRelated, id=rel.id)
    assert queried_rel.start.id == thing2.id
    assert queried_rel.end.id == thing3.id


def test_update_relationship_missing_endpoints(manager):
    # same as test_update_relationship_end_points, with the difference
    # that the relationship is passed through deserialize(serialize())
    # which strips the start/end references

    thing1 = Thing()
    thing2 = Thing()
    thing3 = Thing()

    manager.save(thing1)
    manager.save(thing2)
    manager.save(thing3)

    rel = IndexedRelated(start=thing1, end=thing2)
    manager.save(rel)

    rel.end = thing3
    manager.save(rel)
    reserialized_rel = manager.deserialize(manager.serialize(rel))
    reserialized_rel.start = thing2
    manager.save(reserialized_rel)

    queried_rel = manager.get(IndexedRelated, id=rel.id)
    assert queried_rel.start.id == thing2.id
    assert queried_rel.end.id == thing3.id


def test_delete_instance_types_remain(manager):
    thing = Thing()
    manager.save(thing)

    manager.delete(thing)

    # we are expecting the type to stay in place
    rows = manager.query("""
        START n=node:persistabletype("id:*")
        MATCH n-[:ISA|INSTANCEOF]->m
        RETURN n""")
    result = set(item for (item,) in rows)
    assert result == {Thing}


def test_delete_class(manager):
    """
    Verify that types can be removed from the database.

    The attributes of the type should be removed.
    Instances of the type should not.
    """
    thing = Thing()
    manager.save(thing)

    manager.delete(Thing)
    manager.delete(TypeSystem)

    rows = manager.query('START n=node(*) RETURN COALESCE(n.id?, n)')
    result = set(item for (item,) in rows)
    assert result == {'TypeSystem', 'Entity', str(thing.id)}


def test_destroy(manager):
    thing1 = Thing()
    thing2 = Thing()

    manager.save(thing1)
    manager.save(thing2)
    manager.save(IndexedRelated(thing1, thing2))

    manager.destroy()

    rows = manager.query('START n=node(*) RETURN count(n)')
    assert next(rows) == (0,)

    queries = (
        'START n=node:persistableype(name="Thing") RETURN n',
        'START r=relationship:indexedrelated(id="spam") RETURN r',
    )

    for query in queries:
        rows = manager.query(query)

        with pytest.raises(cypher.CypherError) as excinfo:
            next(rows)

        assert excinfo.value.exception == 'MissingIndexException'


def test_attributes(manager):

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


def test_relationship(manager):
    thing1 = Thing()
    thing2 = Thing()

    rel = Related(thing1, thing2, str_attr='5cal')

    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    rows = manager.query('''
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


def test_indexed_relationship(manager):
    thing1 = Thing()
    thing2 = Thing()

    rel = IndexedRelated(thing1, thing2)

    manager.save(thing1)
    manager.save(thing2)
    manager.save(rel)

    rows = manager.query('''
        START r = relationship:indexedrelated(id={rel_id})
        MATCH n1 -[r]-> n2
        RETURN n1.id, n2.id
    ''', rel_id=rel.id)

    result = set(rows)

    assert result == {
        (str(thing1.id), str(thing2.id))
    }


def test_get_type_hierarchy(manager):
    obj1 = Thing()  # subclasses Entity
    obj2 = Colouring()  # subclass of Thing
    obj3 = Carmine()  # subclass of Colouring

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


def test_get_type_hierarchy_bases_order(manager):
    # before we introduced 'base_index' on IsA, this test would fail because
    # we removed and re-added one of the IsA relationships, which
    # caused the types to be loaded in the incorrect order
    manager.save(Beetroot)

    is_a_props = manager.type_registry.object_to_dict(IsA())
    is_a_props['base_index'] = 1

    list(manager.query(
        ''' START
                Beetroot=node:persistabletype(id="Beetroot"),
                Flavouring=node:persistabletype(id="Colouring")
            MATCH
                Beetroot -[r:ISA]-> Flavouring
            DELETE r
            CREATE
                Beetroot -[nr:ISA {is_a_props}]-> Flavouring
            RETURN nr
        ''', is_a_props=is_a_props))

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
    obj = Thing()
    manager.save(obj)

    query_str = """
        START base = node(*)
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
    """

    rows = manager.query(query_str)
    result = set(rows)

    assert result == {
        ('Thing', 'IsA', Entity),
        (str(obj.id), 'InstanceOf', Thing),
    }


def test_type_hierarchy_diamond(manager):
    beetroot = Beetroot()
    manager.save(beetroot)

    carmine = Carmine()
    manager.save(carmine)

    query_str = """
        START base = node(*)
        MATCH obj -[r:ISA|INSTANCEOF]-> base
        RETURN COALESCE(obj.id?, obj) , r.__type__, base
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


def test_add_type_creates_index(manager):
    manager.save(Thing)

    # this should not raise a MissingIndex error
    result = list(manager.query('START n=node:thing("id:*") RETURN n'))

    assert result == []


def count(manager, type_):
    type_id = type_.__name__
    query = """
        START Thing=node:persistabletype(id="{}")
        MATCH (n)-[:INSTANCEOF]->Thing
        RETURN count(n);
        """.format(type_id)
    rows = manager.query(query)
    (count,) = next(rows)
    return count


def test_save(manager):
    obj = Thing()
    manager.save(obj)
    assert count(manager, Thing) == 1


def test_save_new(manager):
    obj1 = Thing()
    obj2 = Thing()
    manager.save(obj1)
    manager.save(obj2)
    assert count(manager, Thing) == 2


def test_save_replace(manager):
    obj1 = Thing()
    obj2 = Thing()

    obj2.id = obj1.id
    manager.save(obj1)
    manager.save(obj2)
    assert count(manager, Thing) == 1


def test_save_update(manager):
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
    manager.save(Thing)

    query_str = """
        START Thing = node:persistabletype(id="Thing")
        MATCH attr -[DECLAREDON]-> Thing
        RETURN attr
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


def test_attribute_creation(manager):
    """
    Verify that attributes are added to the database when a type is added.
    """
    manager.save(Thing)

    query_str = """
        START Thing = node:persistabletype(id="Thing")
        MATCH attr -[:DECLAREDON]-> Thing
        RETURN attr.__type__, attr.name, attr.unique
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


def test_attribute_inheritance(manager):
    """
    Verify that attributes are created correctly according to type
    inheritence.
    """

    manager.save(Beetroot)

    # ``natural`` on ``Beetroot`` will be found twice by this query, because
    # there are two paths from it to ``Entity``.
    query_str = """
        START Entity = node:persistabletype(id="Entity")
        MATCH attr -[:DECLAREDON]-> type -[:ISA*]-> Entity
        RETURN type.id, attr.name, attr.__type__, attr.default?
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
        START Beetroot = node:persistabletype(id="Beetroot")
        MATCH attr -[:DECLAREDON]-> Beetroot
        RETURN count(attr)
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


# class attributes

def test_serialize_class(manager):
    cls_dict = manager.serialize(ClassAttrThing)
    assert cls_dict == {
        '__type__': 'PersistableType',
        'id': 'ClassAttrThing',
        'cls_attr': 'spam',
    }


def test_serialize_obj(manager):
    instance = ClassAttrThing()

    instance_dict = manager.serialize(instance)
    assert 'cls_attr' not in instance_dict


def test_attr():
    assert ClassAttrThing.cls_attr == 'spam'
    assert ClassAttrThing().cls_attr == 'spam'


def test_load_class_attr(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid(unique=True)
            cls_attr = "spam"

    manager.save_collected_classes(classes)
    manager.reload_types()

    data = {
        '__type__': 'PersistableType',
        'id': 'DynamicClassAttrThing',
        'cls_attr': 'ham'
    }
    cls = manager.deserialize(data)
    assert cls.cls_attr == 'ham'


def test_class_att_overriding(manager):
    with collector() as classes:
        class A(Entity):
            id = Uuid()
            cls_attr = "spam"

        class B(A):
            cls_attr = "ham"

        class C(B):
            pass

    manager.save_collected_classes(classes)
    manager.reload_types()

    a = A()
    b = B()
    c = C()

    assert a.cls_attr == "spam"
    assert b.cls_attr == "ham"
    assert c.cls_attr == "ham"

    manager.save(a)
    manager.save(b)
    manager.save(c)

    query_str = join_lines(
        "START",
        get_start_clause(A, 'A', manager.type_registry),
        """
            MATCH node -[:INSTANCEOF]-> () -[:ISA*0..]-> A
            return node
        """
    )

    results = list(manager.query(query_str))

    for col, in results:
        assert col.cls_attr == col.__class__.cls_attr


def test_class_attr_inheritence(manager):
    with collector() as classes:
        class A(Entity):
            attr = True

        class B(A):
            pass

        class C(B):
            attr = False

        class D(C):
            pass

    manager.save_collected_classes(classes)

    assert A().attr is True
    assert B().attr is True
    assert C().attr is False
    assert D().attr is False


def test_reserved_attribute_name():
    with pytest.raises(TypeError):
        class Nope(Entity):
            __type__ = String()


def test_class_attr_class_serialization(manager):
    with collector() as classes:
        class A(Entity):
            id = Uuid()
            cls_attr = "spam"

        class B(A):
            cls_attr = "ham"

        class C(B):
            pass

    manager.save_collected_classes(classes)

    # we want inherited attributes when we serialize
    assert manager.serialize(C) == {
        '__type__': 'PersistableType',
        'id': 'C',
        'cls_attr': 'ham',
    }

    # we don't want inherited attributes in the db
    query_str = join_lines(
        "START",
        get_start_clause(C, 'C', manager.type_registry),
        """
            RETURN C
        """
    )

    (db_attrs,) = next(manager._execute(query_str))
    properties = db_attrs.get_properties()
    assert 'cls_attr' not in properties


def test_false_class_attr(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()
            cls_attr = False

    manager.save_collected_classes(classes)

    # we want inherited attributes when we serialize
    assert manager.serialize(DynamicClassAttrThing) == {
        '__type__': 'PersistableType',
        'id': 'DynamicClassAttrThing',
        'cls_attr': False,
    }

    manager.reload_types()
    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    thing = DynamicClassAttrThing()

    assert DynamicClassAttrThing.cls_attr is False
    assert thing.cls_attr is False


def test_true_class_attr(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()
            cls_attr = True

    manager.save_collected_classes(classes)

    # we want inherited attributes when we serialize
    assert manager.serialize(DynamicClassAttrThing) == {
        '__type__': 'PersistableType',
        'id': 'DynamicClassAttrThing',
        'cls_attr': True,
    }

    manager.reload_types()
    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    thing = DynamicClassAttrThing()

    assert DynamicClassAttrThing.cls_attr is True
    assert thing.cls_attr is True


def test_edit_class_attrs(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()
            cls_attr = "spam"

    manager.save_collected_classes(classes)

    del DynamicClassAttrThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    assert DynamicClassAttrThing.cls_attr == "spam"

    DynamicClassAttrThing.cls_attr = "ham"
    manager.save(DynamicClassAttrThing)

    del DynamicClassAttrThing

    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    assert DynamicClassAttrThing.cls_attr == "ham"


def test_add_class_attrs(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()

    manager.save_collected_classes(classes)

    del DynamicClassAttrThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')

    DynamicClassAttrThing.cls_attr = "ham"
    manager.save(DynamicClassAttrThing)

    del DynamicClassAttrThing

    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    assert DynamicClassAttrThing.cls_attr == "ham"


def test_delete_class_attrs(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()
            cls_attr = "spam"

    manager.save_collected_classes(classes)

    del DynamicClassAttrThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    assert DynamicClassAttrThing.cls_attr == "spam"

    delattr(DynamicClassAttrThing, 'cls_attr')
    manager.save(DynamicClassAttrThing)

    del DynamicClassAttrThing

    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')
    assert not(hasattr(DynamicClassAttrThing, 'cls_attr'))


def test_add_class_attrs_does_not_create_duplicate_types(manager):
    with collector() as classes:
        class DynamicClassAttrThing(Entity):
            id = Uuid()
    manager.save_collected_classes(classes)
    del DynamicClassAttrThing
    manager.reload_types()

    DynamicClassAttrThing = manager.type_registry.get_class_by_id(
        'DynamicClassAttrThing')

    DynamicClassAttrThing.cls_attr = "ham"
    manager.save(DynamicClassAttrThing)

    rows = manager.query(
        ''' START base = node(*)
            MATCH tpe -[r:ISA]-> base
            RETURN tpe.id , r.__type__, base.id
            ORDER BY tpe.id, base.id
        ''')
    result = list(rows)

    assert result == [
        ('DynamicClassAttrThing', 'IsA', 'Entity'),
    ]


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

    manager.type_registry.register(SubShrub, dynamic=True)
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


def test_class_name_escaping(manager):
    with collector() as classes:
        class Match(Entity):
            id = Uuid()
            where = Uuid()

        class Set(Match):
            pass

        class Return(Set):
            pass

    manager.save_collected_classes(classes)


def test_class_and_attr_name_clash(manager):
    with collector() as classes:
        class Foo(Entity):
            Foo = Uuid()

    manager.save_collected_classes(classes)


def test_type_system_version(manager):
    forced_uuid = uuid.uuid4().hex

    with patch('kaiso.persistence.uuid') as patched_uuid:
        patched_uuid.uuid4().hex = forced_uuid
        manager.invalidate_type_system()

    assert manager._type_system_version() == forced_uuid

    manager.invalidate_type_system()
    assert manager._type_system_version() != forced_uuid


def test_type_system_reload(manager_factory):
    manager_factory(skip_type_loading=True).destroy()
    manager1 = manager_factory()
    manager2 = manager_factory()

    manager1.save(Thing)
    manager2.reload_types()

    type_id = get_type_id(Thing)
    assert manager2.type_registry.get_class_by_id(type_id) == Thing

    Thing.cls_attr = "cls_attr"
    manager1.save(Thing)
    manager2.reload_types()

    descriptor = manager2.type_registry.get_descriptor_by_id(type_id)
    assert "cls_attr" in descriptor.class_attributes


def test_reload_external_changes(manager, connection):

    manager.save(Thing)
    manager.reload_types()  # cache type registry

    # update the graph as an external manager would
    # (change a value and bump the typesystem version)
    start_clauses = (
        get_start_clause(Thing, 'Thing', manager.type_registry),
        get_start_clause(manager.type_system, 'ts', manager.type_registry),
    )
    query = join_lines(
        'START',
        (start_clauses, ','),
        'SET ts.version = {version}',
        'SET Thing.cls_attr = {cls_attr}',
        'RETURN Thing'
    )
    query_params = {
        'cls_attr': 'placeholder',
        'version': str(uuid.uuid4())
    }
    cypher.execute(connection, query, query_params)

    # reloading types should see the difference
    manager.reload_types()
    descriptor = manager.type_registry.get_descriptor(Thing)
    assert "cls_attr" in descriptor.class_attributes


def test_invalidate_type_system(manager):

    with collector():
        class TypeA(Entity):
            id = Uuid(unique=True)

        class TypeB(Entity):
            attr = String(unique=True)

        class BaseType(Entity):
            pass

    manager.type_registry.register(TypeA, dynamic=True)
    manager.type_registry.register(TypeB, dynamic=True)
    manager.type_registry.register(BaseType, dynamic=True)

    versions = []

    def is_distinct_version(v):
        distinct = v not in versions
        versions.append(v)
        return distinct

    v0 = manager._type_system_version()
    assert is_distinct_version(v0)

    manager.save(TypeA)  # create type
    manager.save(BaseType)
    v1 = manager._type_system_version()
    assert is_distinct_version(v1)

    manager.save(TypeA)  # save unchanged type
    assert manager._type_system_version() == v1

    type_a = TypeA()
    manager.save(type_a)  # create instance
    v2 = manager._type_system_version()
    assert is_distinct_version(v2)

    type_b = TypeB(attr="value")
    manager.save(type_b)  # create instance & type
    v3 = manager._type_system_version()
    assert is_distinct_version(v3)

    type_b.new_attr = "value"
    manager.save(type_b)  # add instance attribute
    assert manager._type_system_version() == v3

    type_b.new_attr = "new_value"
    manager.save(type_b)  # modify instance attribute
    assert manager._type_system_version() == v3

    del type_b.new_attr
    manager.save(type_b)  # delete instance attribute
    assert manager._type_system_version() == v3

    isa = IsA(type_a, BaseType)
    isa.base_index = 1
    manager.save(isa)  # create a type-hierarchy relationship
    v4 = manager._type_system_version()
    assert is_distinct_version(v4)

    rel = Related(type_a, type_b)
    manager.save(rel)  # create a non-type-hierarchy relationship
    assert manager._type_system_version() == v4

    manager.update_type(TypeA, (BaseType,))  # reparent
    v5 = manager._type_system_version()
    assert is_distinct_version(v5)

    # update_type reloads the type hierarchy, so refresh references
    TypeA = manager.type_registry.get_class_by_id('TypeA')
    TypeB = manager.type_registry.get_class_by_id('TypeB')
    BaseType = manager.type_registry.get_class_by_id('BaseType')

    manager.delete(isa)  # delete a type-hierarchy relationship
    v6 = manager._type_system_version()
    assert is_distinct_version(v6)

    manager.delete(rel)  # delete a non-type-hierarchy relationship
    assert manager._type_system_version() == v6

    manager.delete(TypeA)  # delete type
    v7 = manager._type_system_version()
    assert is_distinct_version(v7)

    manager.delete(type_a)  # delete instance
    assert manager._type_system_version() == v7

    TypeB.cls_attr = "value"
    manager.save(TypeB)  # add class attribute
    v8 = manager._type_system_version()
    assert is_distinct_version(v8)

    TypeB.cls_attr = "new_value"
    manager.save(TypeB)  # modify class attribute
    v9 = manager._type_system_version()
    assert is_distinct_version(v9)

    del TypeB.cls_attr
    manager.save(TypeB)  # delete class attribute
    v10 = manager._type_system_version()
    assert is_distinct_version(v10)
