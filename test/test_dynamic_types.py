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
def test_modify_dynamic_type(storage):

    attrs = {'id': String(unique=True)}
    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)
    storage.save(Foobar)

    Foobar.ham = String(default='eggs')
    print '-------------------'
    print storage.get_diff(Foobar)

    storage.save(Foobar)

    rows = storage.query(
        'START n=node:persistablemeta(id="Foobar") '
        'MATCH n <-[:DECLAREDON]- attr '
        'RETURN count(attr)')

    (count,) = next(rows)

    assert count == 2


@pytest.mark.usefixtures('storage')
def _test_modify_dynamic_type_via_2nd_storage(storage):

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
