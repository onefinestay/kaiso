from mock import ANY
from kaiso.types import Entity
from kaiso.attributes import Uuid, String, Bool


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


def test_has_code_defined_attribute(type_registry):
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}

    # test purely dynamic type
    NewType = type_registry.create_type("NewType", (Entity,), attrs)
    assert not type_registry.has_code_defined_attribute(NewType, "extra")
    assert not type_registry.has_code_defined_attribute(NewType, "nonexistant")

    # test purely code-defined type
    class BarType(Entity):
        extra = String()
    type_registry.register(BarType)
    assert type_registry.has_code_defined_attribute(BarType, "extra")
    assert not type_registry.has_code_defined_attribute(BarType, "nonexistant")

    # augment FooType with an "extra" attr; redefine the "id" attr
    type_registry.create_type("FooType", (Entity,), attrs)

    # test augmented type
    assert type_registry.has_code_defined_attribute(FooType, "id")
    assert not type_registry.has_code_defined_attribute(FooType, "extra")
    assert not type_registry.has_code_defined_attribute(FooType, "nonexistant")

    # create static type with a dynamic subclass
    class BazType(Entity):
        name = String()
        special = Bool()
    type_registry.register(BazType)

    attrs = {'special': Bool(), 'extra': Bool()}
    SubBazType = type_registry.create_type("SubBazType", (BazType,), attrs)

    # test the static type
    assert type_registry.has_code_defined_attribute(BazType, "name")
    assert type_registry.has_code_defined_attribute(BazType, "special")

    # test the subtype does not override it
    assert type_registry.has_code_defined_attribute(SubBazType, "name")
    assert type_registry.has_code_defined_attribute(SubBazType, "special")
    assert not type_registry.has_code_defined_attribute(SubBazType, "extra")


def test_get_registered_types(type_registry):
    initial_classes = set(type_registry.get_registered_types())
    assert Entity in initial_classes

    # nothing should change about the set of registered types
    # when creating one through the create_type API
    type_registry.create_type("FooType", (), {})
    classes = set(type_registry.get_registered_types())

    assert initial_classes == classes
