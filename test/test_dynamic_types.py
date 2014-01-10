import pytest

from kaiso.attributes import String
from kaiso.exceptions import UnknownType
from kaiso.types import Entity


def test_static_types_can_be_augmented(manager):
    # make sure static-type Entity is in the graph
    manager.save(Entity)

    # create an entity instance, attempting to set a non-defined attribute,
    # and check that the attribute has not been saved to the graph
    entity1 = Entity(foo="foo")
    manager.save(entity1)
    rows = manager.query("""
        START n=node:persistabletype(id="Entity")
        MATCH n <-[:INSTANCEOF]- entity
        RETURN entity
    """)
    (loaded1, ) = next(rows)
    assert not hasattr(loaded1, 'foo')

    # Define and save a new Entity attribute.
    Entity.foo = String()
    manager.save(Entity)
    # Delete attribute and let `reload_types` restore it
    del Entity.foo
    manager.reload_types()

    # Create an entity with a value for the new attribute.
    # Check that it has now been stored in the graph.
    entity2 = Entity(foo="foo")
    manager.save(entity2)
    rows = manager.query("""
    START n=node:persistabletype(id="Entity")
        MATCH n <-[:INSTANCEOF]- entity
        WHERE entity.foo! = "foo"
        RETURN entity
    """)
    rows = list(rows)
    assert len(rows) == 1
    (loaded2, ) = rows[0]
    assert loaded2.foo == 'foo'


def test_save_dynamic_type(manager):

    attrs = {'id': String(unique=True)}
    Foobar = manager.create_type('Foobar', (Entity,), attrs)

    manager.save(Foobar)

    rows = manager.query('START n=node:persistabletype(id="Foobar") RETURN n')
    (result,) = next(rows)

    assert result is Foobar


def test_save_dynamic_typed_obj(manager):

    attrs = {'id': String(unique=True)}
    Foobar = manager.create_type('Foobar', (Entity,), attrs)
    manager.save(Foobar)

    foo = Foobar(id='spam')
    manager.save(foo)

    rows = manager.query('START n=node:foobar(id="spam") RETURN n')
    (result,) = next(rows)

    assert result.id == foo.id


def test_add_attr_to_type(manager):
    Foobar = manager.create_type('Foobar', (Entity,), {})
    manager.save(Foobar)

    Foobar.ham = String(default='eggs')
    manager.save(Foobar)

    rows = manager.query(
        'START n=node:persistabletype(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 1


def test_remove_attr_from_type(manager):
    attrs = {'ham': String()}
    Foobar = manager.create_type('Foobar', (Entity,), attrs)
    manager.save(Foobar)

    del Foobar.ham

    manager.save(Foobar)

    rows = manager.query(
        'START n=node:persistabletype(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 0


def test_removing_attr_from_declared_type_does_not_remove_it(manager):

    # the use case:
    # developer defines a partial type in code
    # the type Ham gets updated in the DB at runtime
    # then the code changes, removing an attribute from the class
    # the type definition in the DB is not affected, nor
    # the data gets back from the DB

    class Ham(Entity):
        egg = String

    manager.save(Ham)

    attrs = {'egg': String(), 'spam': String()}
    DynHam = manager.create_type('Ham', (Entity,), attrs)
    manager.save(DynHam)

    del Ham.egg
    manager.save(Ham)

    rows = manager.query(
        'START n=node:persistabletype(id="Ham") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 2


def test_load_dynamic_types(manager):
    Animal = manager.create_type('Animal', (Entity,), {'id': String()})
    Horse = manager.create_type('Horse', (Animal,), {'hoof': String()})
    Duck = manager.create_type('Duck', (Animal,), {'beek': String()})
    Beaver = manager.create_type('Beaver', (Animal,), {'tail': String()})
    Platypus = manager.create_type(
        'Platypus', (Duck, Beaver), {'egg': String()})

    manager.save(Horse)
    manager.save(Platypus)

    # this is the same as creating a new manager
    manager.reload_types()

    rows = manager.query(
        '''
        START ts=node:typesystem(id="TypeSystem")
        MATCH p = (ts -[:DEFINES]-> () <-[:ISA*0..]- tpe),
            tpe <-[:DECLAREDON*0..]- attr,
            tpe -[:ISA*0..1]-> base
        RETURN tpe.id,  length(p) AS level,
            filter(b_id in collect(distinct base.id): b_id <> tpe.id),
            collect(distinct attr.name?)
        ORDER BY level, tpe.id
        ''')
    result = list(rows)

    assert result == [
        ('Entity', 1, [], []),
        ('Animal', 2, ['Entity'], ['id']),
        ('Beaver', 3, ['Animal'], ['tail']),
        ('Duck', 3, ['Animal'], ['beek']),
        ('Horse', 3, ['Animal'], ['hoof']),
        ('Platypus', 4, ['Duck', 'Beaver'], ['egg']),
    ]


def test_add_attr_to_type_via_2nd_manager(manager):
    attrs = {'id': String(unique=True)}
    Shrub = manager.create_type('Shrub', (Entity,), attrs)
    manager.save(Shrub)

    shrub = Shrub(id='spam')
    manager.save(shrub)

    # this is the same as creating a new manager using the same URL
    manager.reload_types()

    (Shrub,) = next(manager.query(
        'START cls=node:persistabletype(id="Shrub") RETURN cls'))
    Shrub.newattr = String(default='eggs')
    manager.save(Shrub)

    # we want to query from an independent manager
    manager.reload_types()
    rows = manager.query('START n=node:shrub(id="spam") RETURN n')
    (result,) = next(rows)

    assert result.newattr is None


def test_type_registry_independence(manager):
    Shrub = manager.create_type('Shrub', (Entity,), {})
    manager.type_registry.get_descriptor(Shrub)

    manager.reload_types()
    with pytest.raises(UnknownType):
        manager.type_registry.get_descriptor(Shrub)
