import pytest

from kaiso.exceptions import TypeAlreadyRegistered
from kaiso.types import Entity, PersistableMeta


def test_register_duplicate_fails():
    class Duplicate(Entity):
        pass

    with pytest.raises(TypeAlreadyRegistered):
        # same as declaring a class
        PersistableMeta('Duplicate', (Entity,), {})
        assert Duplicate is None
