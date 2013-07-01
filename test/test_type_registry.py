from mock import ANY
from kaiso.types import Entity
from kaiso.attributes import Uuid, String


class FooType(Entity):
    id = Uuid(unique=True)


def test_get_clsss_by_id_returns_static_type(type_registry):

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    type_registry.create_type("FooType", (), attrs)

    # check that cls_id 'FooType' gives us the static FooType
    assert type_registry.get_class_by_id("FooType") == FooType


def test_get_descriptor_returns_dynamic_type(type_registry):

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    DynamicFooType = type_registry.create_type("FooType", (), attrs)

    # check that the descriptor for FooType is the same as the descriptor for
    # DynamicFooType
    dynamic_descriptor = type_registry.get_descriptor(DynamicFooType)
    assert type_registry.get_descriptor_by_id("FooType") == dynamic_descriptor

    # check that the descriptor contains DynamicFooType's extra attributes
    assert "extra" in type_registry.get_descriptor_by_id("FooType").attributes


def test_get_index_entries(type_registry):

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    DynamicFooType = type_registry.create_type("FooType", (), attrs)

    # check index entries for the types
    assert list(type_registry.get_index_entries(FooType)) == [
        ('persistabletype', 'id', 'FooType')
    ]
    assert list(type_registry.get_index_entries(DynamicFooType)) == [
        ('persistabletype', 'id', 'FooType')
    ]

    # create an instance
    foo = DynamicFooType()
    foo.extra = "hello"

    # check that indices are returned for both unique attributes
    # checl that 'extra' has a value
    index_entries = list(type_registry.get_index_entries(foo))
    assert len(index_entries) == 2
    assert index_entries[0][0] == "footype"
    assert index_entries[0][1] == "id"
    assert index_entries[0][2] == ANY  # id not known
    assert index_entries[1][0] == "footype"
    assert index_entries[1][1] == "extra"
    assert index_entries[1][2] == "hello"


def test_is_registered(type_registry):

    assert type_registry.is_registered(FooType) is True
    assert type_registry.is_registered("FooType") is True


def test_is_dynamic_type(type_registry):

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    DynamicFooType = type_registry.create_type("FooType", (), attrs)

    assert type_registry.is_dynamic_type(DynamicFooType) is True
    assert type_registry.is_dynamic_type(FooType) is True


def test_get_registered_types(type_registry):
    initial_classes = set(type_registry.get_registered_types())
    assert Entity in initial_classes

    # nothing should change about the set of registered types
    # when creating one through the create_type API
    type_registry.create_type("FooType", (), {})
    classes = set(type_registry.get_registered_types())

    assert initial_classes == classes
