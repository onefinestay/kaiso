import pytest

from kaiso.attributes import String, Uuid
from kaiso.queries import get_match_clause
from kaiso.types import Entity, collector


@pytest.fixture
def static_types(manager):
    class Thing(Entity):
        id = Uuid(unique=True)
        cls_attr = "spam"

    manager.save(Thing)

    return {
        'Thing': Thing,
    }


def test_serialize_class(manager, static_types):
    Thing = static_types['Thing']

    cls_dict = manager.serialize(Thing)
    assert cls_dict == {
        '__type__': 'PersistableType',
        'id': 'Thing',
        'cls_attr': 'spam',
    }


def test_serialize_obj(manager, static_types):
    Thing = static_types['Thing']

    instance = Thing()

    instance_dict = manager.serialize(instance)
    assert 'cls_attr' not in instance_dict


def test_attr(static_types):
    Thing = static_types['Thing']

    assert Thing.cls_attr == 'spam'
    assert Thing().cls_attr == 'spam'


def test_load_class_attr(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid(unique=True)
            cls_attr = "spam"

    manager.save_collected_classes(classes)
    manager.reload_types()

    data = {
        '__type__': 'PersistableType',
        'id': 'DynamicThing',
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

    query_str = """
        MATCH
        {},
        (node)-[:INSTANCEOF]->()-[:ISA*0..]->A
        RETURN node
    """.format(
        get_match_clause(A, 'A', manager.type_registry),
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
    query_str = "MATCH {} RETURN C".format(
        get_match_clause(C, 'C', manager.type_registry),
    )

    (db_attrs,) = next(manager._execute(query_str))
    properties = db_attrs.get_properties()
    assert 'cls_attr' not in properties


def test_false_class_attr(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()
            cls_attr = False

    manager.save_collected_classes(classes)

    # we want inherited attributes when we serialize
    assert manager.serialize(DynamicThing) == {
        '__type__': 'PersistableType',
        'id': 'DynamicThing',
        'cls_attr': False,
    }

    manager.reload_types()
    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    thing = DynamicThing()

    assert DynamicThing.cls_attr is False
    assert thing.cls_attr is False


def test_true_class_attr(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()
            cls_attr = True

    manager.save_collected_classes(classes)

    # we want inherited attributes when we serialize
    assert manager.serialize(DynamicThing) == {
        '__type__': 'PersistableType',
        'id': 'DynamicThing',
        'cls_attr': True,
    }

    manager.reload_types()
    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    thing = DynamicThing()

    assert DynamicThing.cls_attr is True
    assert thing.cls_attr is True


def test_edit_class_attrs(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()
            cls_attr = "spam"

    manager.save_collected_classes(classes)

    del DynamicThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    assert DynamicThing.cls_attr == "spam"

    DynamicThing.cls_attr = "ham"
    manager.save(DynamicThing)

    del DynamicThing

    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    assert DynamicThing.cls_attr == "ham"


def test_add_class_attrs(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()

    manager.save_collected_classes(classes)

    del DynamicThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')

    DynamicThing.cls_attr = "ham"
    manager.save(DynamicThing)

    del DynamicThing

    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    assert DynamicThing.cls_attr == "ham"


def test_delete_class_attrs(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()
            cls_attr = "spam"

    manager.save_collected_classes(classes)

    del DynamicThing

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    assert DynamicThing.cls_attr == "spam"

    delattr(DynamicThing, 'cls_attr')
    manager.save(DynamicThing)

    del DynamicThing

    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')
    assert not(hasattr(DynamicThing, 'cls_attr'))


def test_add_class_attrs_does_not_create_duplicate_types(manager):
    with collector() as classes:
        class DynamicThing(Entity):
            id = Uuid()
    manager.save_collected_classes(classes)
    del DynamicThing
    manager.reload_types()

    DynamicThing = manager.type_registry.get_class_by_id(
        'DynamicThing')

    DynamicThing.cls_attr = "ham"
    manager.save(DynamicThing)

    rows = manager.query(
        ''' START base = node(*)
            MATCH tpe -[r:ISA]-> base
            RETURN tpe.id , r.__type__, base.id
            ORDER BY tpe.id, base.id
        ''')
    result = list(rows)

    assert result == [
        ('DynamicThing', 'IsA', 'Entity'),
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
