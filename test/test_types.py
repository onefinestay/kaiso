import pytest

from kaiso.exceptions import TypeAlreadyRegistered
from kaiso.types import Entity, TypeRegistry


def test_register_duplicate_dynamic():

    type_registry = TypeRegistry()
    type_registry.initialize()

    with pytest.raises(TypeAlreadyRegistered):
        # this declares a dynamic type
        type_registry.create('Duplicate', (Entity,), {})
        type_registry.create('Duplicate', (Entity,), {})


def test_register_duplicate_static():

    # this declares a static class
    class Duplicate(Entity):
        pass

    type_registry = TypeRegistry()
    type_registry.initialize()

    with pytest.raises(TypeAlreadyRegistered):

        # this also declares a static class
        DuplicateDuplicate = type("Duplicate", (Entity,), {})
        type_registry.register(DuplicateDuplicate)
