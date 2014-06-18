from kaiso.attributes import Integer
from kaiso.types import collector, Entity


def test_dynamic_unique_attr_adds_label(manager):
    with collector() as collected:
        class Foo(Entity):
            attr = Integer(unique=False)

        class Bar(Entity):
            attr = Integer(unique=True)

    manager.save_collected_classes(collected)

    assert manager.type_registry.get_labels_for_type(Foo) == set()
    assert manager.type_registry.get_labels_for_type(Bar) == set(['Bar'])


def test_static_class_adds_label(manager):
    class Bar(Entity):
        pass

    Bar  # pyflakes

    with collector() as collected:
        class Foo(Entity):
            pass

        class Bar(Entity):
            pass

    manager.save_collected_classes(collected)

    assert manager.type_registry.get_labels_for_type(Foo) == set()
    assert manager.type_registry.get_labels_for_type(Bar) == set(['Bar'])
