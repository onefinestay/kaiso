from kaiso.types import collected
from kaiso.types import PersistableType, Entity
from kaiso.attributes import String


def test_class_collection():

    class CollectFoo():
        __metaclass__ = PersistableType

    class CollectBar(Entity):
        baz = String()

    assert "CollectFoo" in collected
    assert "CollectBar" in collected
    assert "Entity" in collected
    assert "String" in collected
