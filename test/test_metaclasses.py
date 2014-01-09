from kaiso.types import collected_static_classes
from kaiso.types import PersistableType, Entity
from kaiso.attributes import String


def test_class_collection():

    class CollectFoo():
        __metaclass__ = PersistableType

    class CollectBar(Entity):
        baz = String()

    assert "CollectFoo" in collected_static_classes.get_classes()
    assert "CollectBar" in collected_static_classes.get_classes()
    assert "Entity" in collected_static_classes.get_classes()
    assert "String" in collected_static_classes.get_classes()
