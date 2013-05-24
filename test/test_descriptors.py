import pytest

from kaiso.types import Entity
from kaiso.exceptions import UnknownType


class Ente(Entity):
    pass


def test_type_ids(type_registry):

    descr = type_registry.get_descriptor(Entity)
    assert descr.type_id == 'Entity'

    descr = type_registry.get_descriptor(Ente)
    assert descr.type_id == 'Ente'


def test_unknown_type(type_registry):
    with pytest.raises(UnknownType):
        type_registry.get_descriptor(UnknownType)
