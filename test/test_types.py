import pytest

from kaiso.exceptions import TypeAlreadyRegistered, TypeAlreadyCollected
from kaiso.types import Entity, TypeRegistry


def test_register_duplicate():

    type_registry = TypeRegistry()

    with pytest.raises(TypeAlreadyRegistered):
        # this declares a dynamic type
        type_registry.create_type('Duplicate', (Entity,), {})
        type_registry.create_type('Duplicate', (Entity,), {})


def test_collect_duplicate():

    # this declares a static class
    class Duplicate(Entity):
        pass

    with pytest.raises(TypeAlreadyCollected):
        # this also declares a static class
        type("Duplicate", (Entity,), {})
