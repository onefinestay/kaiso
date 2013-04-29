import pytest

from kaiso.attributes import String
from kaiso.types import Entity
from kaiso.persistence import Storage


@pytest.mark.usefixtures('storage')
def test_save_dynamic_type(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)

    storage.save(Foobar)

    rows = storage.query('START n=node:persistablemeta(id="Foobar") RETURN n')
    (result,) = next(rows)

    assert result is Foobar


@pytest.mark.usefixtures('storage')
def test_save_dynamic_typed_obj(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)

    foo = Foobar(id='spam')
    storage.save(foo)

    rows = storage.query('START n=node:foobar(id="spam") RETURN n')
    (result,) = next(rows)

    assert result.id == foo.id


@pytest.mark.usefixtures('storage')
def test_add_attr_to_type(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)
    storage.save(Foobar)

    Foobar.ham = String(default='eggs')
    storage.save(Foobar)

    rows = storage.query(
        'START n=node:persistablemeta(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 2


@pytest.mark.usefixtures('storage')
def test_remove_attr_from_type(storage):
    attrs = {'id': String(unique=True), 'ham': String()}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)
    storage.save(Foobar)

    del Foobar.ham
    storage.save(Foobar)

    rows = storage.query(
        'START n=node:persistablemeta(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')
    (count,) = next(rows)
    assert count == 1


@pytest.mark.usefixtures('storage')
def test_add_attr_to_type_via_2nd_storage(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)

    foo = Foobar(id='spam')
    storage.save(foo)

    storage2 = Storage(storage._conn_uri)
    attrs = {'id': String(unique=True), 'ham': String(default='eggs')}
    Foobar = storage2.dynamic_type('Foobar', (Entity,), attrs)

    storage2.save(Foobar)
    rows = storage.query('START n=node:foobar(id="spam") RETURN n')
    (result,) = next(rows)

    assert result.ham == 'eggs'
