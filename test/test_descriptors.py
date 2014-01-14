import pytest

from kaiso.attributes import String
from kaiso.exceptions import UnknownType
from kaiso.types import Entity, TypeRegistry, collector


def test_unknown_type(type_registry):
    with pytest.raises(UnknownType):
        type_registry.get_descriptor(UnknownType)


def test_static_descriptor_caching(manager):
    with collector():

        class Thing(Entity):
            prop_x = String(required=True)

        class That(Entity):
            prop_y = String(unique=True)

        type_registry = TypeRegistry()

        thing_descriptor1 = type_registry.get_descriptor(Thing)
        thing_descriptor2 = type_registry.get_descriptor(Thing)

        that_descriptor1 = type_registry.get_descriptor(That)
        that_descriptor2 = type_registry.get_descriptor(That)

        # assert repeated calls return the same objects
        assert thing_descriptor1 is thing_descriptor2
        assert that_descriptor1 is that_descriptor2

        # check that different types still get different descriptors
        assert thing_descriptor1 is not that_descriptor1


def test_descriptor_property_caching(manager):
    with collector():

        class Thing(Entity):
            prop_x = String(required=True)

        class That(Entity):
            prop_y = String(unique=True)

        type_registry = TypeRegistry()

        thing_descriptor = type_registry.get_descriptor(Thing)
        that_descriptor = type_registry.get_descriptor(That)

        property_names = [
            'attributes', 'relationships', 'declared_attributes',
            'class_attributes', 'declared_class_attributes'
        ]
        for name in property_names:
            thing_val1 = getattr(thing_descriptor, name)
            thing_val2 = getattr(thing_descriptor, name)
            that_val = getattr(that_descriptor, name)
            assert thing_val1 is thing_val2, name
            assert thing_val2 is not that_val, name
