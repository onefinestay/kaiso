import pytest

from kaiso.attributes import String, Uuid
from kaiso.types import PersistableMeta, Entity
from kaiso.exceptions import UnknownType


@pytest.mark.usefixtures('storage')
def test_add_dynamic_type(storage):

    attrs = {
        'id': String(default='spam')
    }

    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)

    storage.save(Foobar)

    result = set(storage.query(
        'START n=node:persistablemeta("id:*") RETURN n'))

    assert result == {(Entity,), (Foobar,)}

    assert isinstance(Foobar, storage.dynamic_type)
    assert issubclass(Foobar, Entity)

    with pytest.raises(UnknownType):
        PersistableMeta.get_class_by_id('Foobar')

    assert storage.dynamic_type.get_class_by_id('Foobar') is Foobar


@pytest.mark.usefixtures('storage')
def test_add_dynamic_type_obj(storage):

    attrs = {
        'id': String(default='spam', unique=True)
    }

    Foobar = storage.dynamic_type('Foobar', (Entity,), attrs)

    foo = Foobar()
    storage.save(foo)

    (result,) = next(storage.query(
        'START n=node:foobar(id="spam") RETURN n'))

    assert result.id == foo.id
    assert isinstance(result, Foobar)
    assert isinstance(result, Entity)


@pytest.mark.usefixtures('storage')
def test_custom_based_dynamic_type_obj(storage):

    class Spam(Entity):
        id = String(default='spam', unique=True)

    attrs = {
        'ham': Uuid()
    }
    Foobar = storage.dynamic_type('Foobar', (Spam,), attrs)

    foo = Foobar()
    storage.save(foo)

    (result,) = next(storage.query(
        'START n=node:spam(id="spam") RETURN n'))

    assert result.id == foo.id
    assert result.ham == foo.ham
    assert isinstance(result, Foobar)
    assert isinstance(result, Entity)
    assert isinstance(result, Spam)


@pytest.mark.usefixtures('storage')
def test_mix_custom_and_declared_type(storage):

    class Spam(Entity):
        id = String(default='spam', unique=True)

    attrs = {
        'id': String(default='spam', unique=True),
        'ham': Uuid()
    }

    DynSpam = storage.dynamic_type('Spam', (Entity,), attrs)

    foo = DynSpam()
    storage.save(foo)

    (result,) = next(storage.query(
        'START n=node:spam(id="spam") RETURN n'))

    return
    assert result.id == foo.id
    assert result.ham == foo.ham
    assert isinstance(result, Entity)

    assert isinstance(result, Spam)
    assert isinstance(result, DynSpam)
