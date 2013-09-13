import pytest

from kaiso.types import Entity
from kaiso.test_helpers import TemporaryStaticTypes


@pytest.fixture
def bar(temporary_static_types):
    class Bar(Entity):
        pass


def test_context_manager_a():
    with TemporaryStaticTypes():
        class Bar(Entity):
            pass


def test_context_manager_b():
    with TemporaryStaticTypes():
        class Bar(Entity):
            pass


def test_fixture_a(temporary_static_types):
    class Bar(Entity):
        pass


def test_fixture_b(temporary_static_types):
    class Bar(Entity):
        pass


def test_fixture_with_types_a(bar):
    pass


def test_fixture_with_types_b(bar):
    pass
