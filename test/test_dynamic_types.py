import pytest

from kaiso.attributes import String
from kaiso.types import Entity


@pytest.mark.usefixtures('storage')
def test_save_dynamic_type(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.create_type('Foobar', (Entity,), attrs)

    storage.save(Foobar)

    rows = storage.query('START n=node:persistablemeta(id="Foobar") RETURN n')
    (result,) = next(rows)

    assert result is Foobar


@pytest.mark.usefixtures('storage')
def test_save_dynamic_typed_obj(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.create_type('Foobar', (Entity,), attrs)

    foo = Foobar(id='spam')
    storage.save(foo)

    rows = storage.query('START n=node:foobar(id="spam") RETURN n')
    (result,) = next(rows)

    assert result.id == foo.id


@pytest.mark.usefixtures('storage')
def test_add_attr_to_type(storage):
    Foobar = storage.create_type('Foobar', (Entity,), {})
    storage.save(Foobar)

    Foobar.ham = String(default='eggs')
    storage.save(Foobar)

    rows = storage.query(
        'START n=node:persistablemeta(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 1


@pytest.mark.usefixtures('storage')
def test_remove_attr_from_type(storage):
    attrs = {'ham': String()}
    Foobar = storage.create_type('Foobar', (Entity,), attrs)
    storage.save(Foobar)

    del Foobar.ham
    storage.save(Foobar)

    rows = storage.query(
        'START n=node:persistablemeta(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 0


@pytest.mark.usefixtures('storage')
def test_removing_attr_from_declared_type_does_not_remove_it(storage):

    # the use case:
    # developer defines a partial type in code
    # the type Ham gets updated in the DB at runtime
    # then the code changes, removing an attribute from the class
    # the type definition in the DB is not affected, nor
    # the data gets back from the DB

    class Ham(Entity):
        egg = String

    storage.save(Ham)

    attrs = {'egg': String(), 'spam': String()}
    DynHam = storage.create_type('Ham', (Entity,), attrs)
    storage.save(DynHam)

    del Ham.egg
    storage.save(Ham)

    rows = storage.query(
        'START n=node:persistablemeta(id="Ham") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 2


@pytest.mark.usefixtures('storage')
def test_load_dynamic_types(storage):
    Animal = storage.create_type('Animal', (Entity,), {'id': String()})
    Horse = storage.create_type('Horse', (Animal,), {'hoof': String()})
    Duck = storage.create_type('Duck', (Animal,), {'beek': String()})
    Beaver = storage.create_type('Beaver', (Animal,), {'tail': String()})
    Platypus = storage.create_type(
        'Platypus', (Duck, Beaver), {'egg': String()})

    storage.save(Horse)
    storage.save(Platypus)

    # this is the same as creating a new storage
    storage.initialize()

    rows = storage.query(
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


@pytest.mark.usefixtures('storage')
def test_add_attr_to_type_via_2nd_storage(storage):
    attrs = {'id': String(unique=True)}
    Shrub = storage.create_type('Shrub', (Entity,), attrs)

    shrub = Shrub(id='spam')
    storage.save(shrub)

    # this is the same as creating a new storage using the same URL
    storage.initialize()

    (Shrub,) = next(storage.query(
        'START cls=node:persistablemeta(id="Shrub") RETURN cls'))
    Shrub.newattr = String(default='eggs')
    storage.save(Shrub)

    # we want to query from an independent storage
    storage.initialize()
    rows = storage.query('START n=node:shrub(id="spam") RETURN n')
    (result,) = next(rows)

    assert hasattr(result, 'newattr')
