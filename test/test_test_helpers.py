import pytest

from kaiso.types import Entity
from kaiso.test_helpers import TemporaryStaticTypes


@pytest.fixture
def bar(request):
    patcher = TemporaryStaticTypes()
    patcher.start()
    class Bar(Entity):
        pass
    request.addfinalizer(patcher.stop)


def test_context_manager_a():
    with TemporaryStaticTypes():
        class Bar(Entity):
            pass


def test_context_manager_b():
    with TemporaryStaticTypes():
        class Bar(Entity):
            pass


def test_fixture_a(bar):
    pass


def test_fixture_b(bar):
    pass
