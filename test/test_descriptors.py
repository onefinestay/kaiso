import pytest

from kaiso.types import PersistableMeta, Entity
from kaiso.exceptions import UnknownType


class Foo(Entity):
    pass


def test_type_ids():
    descr = PersistableMeta.get_descriptor(PersistableMeta)
    assert descr.type_id == 'PersistableMeta'

    descr = PersistableMeta.get_descriptor(Entity)
    assert descr.type_id == 'Entity'

    descr = PersistableMeta.get_descriptor(Foo)
    assert descr.type_id == 'Foo'


def test_unknown_type():
    with pytest.raises(UnknownType):
        PersistableMeta.get_descriptor(UnknownType)
