import pytest

from kaiso.types import TypeRegistry, Entity
from kaiso.exceptions import UnknownType


class Ente(Entity):
    pass

type_registry = TypeRegistry()
type_registry.initialize()


def test_type_ids():

    descr = type_registry.get_descriptor(Entity)
    assert descr.type_id == 'Entity'

    descr = type_registry.get_descriptor(Ente)
    assert descr.type_id == 'Ente'


def test_unknown_type():
    with pytest.raises(UnknownType):
        type_registry.get_descriptor(UnknownType)
