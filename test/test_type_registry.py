from mock import ANY
import pytest

from kaiso.types import Entity
from kaiso.attributes import Uuid, String, Bool


@pytest.fixture
def static_types(manager):

    class FooType(Entity):
        id = Uuid(unique=True)
        cls_attr = "placeholder"

    class BarType(Entity):
        extra = String()
        cls_attr = "placeholder"

    class BazType(Entity):
        name = String()
        special = Bool()
        cls_attr = "placeholder"

    class AType(Entity):
        foo = String()

    class BType(AType):
        pass

    return {
        'FooType': FooType,
        'BarType': BarType,
        'BazType': BazType,
        'AType': AType,
        'BType': BType,
    }


def test_get_clsss_by_id_returns_static_type(type_registry, static_types):
    FooType = static_types['FooType']

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    type_registry.create_type("FooType", (), attrs)

    # check that cls_id 'FooType' gives us the static FooType
    assert type_registry.get_class_by_id("FooType") == FooType


def test_get_descriptor_returns_dynamic_type(type_registry, static_types):

    # create a dynamic FooType
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    DynamicFooType = type_registry.create_type("FooType", (), attrs)

    # check that the descriptor for FooType is the same as the descriptor for
    # DynamicFooType
    dynamic_descriptor = type_registry.get_descriptor(DynamicFooType)
    assert type_registry.get_descriptor_by_id("FooType") == dynamic_descriptor

    # check that the descriptor contains DynamicFooType's extra attributes
    assert "extra" in type_registry.get_descriptor_by_id("FooType").attributes


def test_get_index_entries(type_registry, static_types):
    FooType = static_types['FooType']

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


def test_is_static_type(type_registry, static_types):

    # test purely static type
    FooType = static_types['FooType']
    assert type_registry.is_static_type(FooType) is True

    # test purely dynamic type
    NewType = type_registry.create_type("NewType", (Entity,), {})
    assert type_registry.is_static_type(NewType) is False

    # augment a static type
    attrs = {'id': Uuid(unique=True), 'extra': String(unique=True)}
    AugmentedFooType = type_registry.create_type("FooType", (), attrs)

    assert type_registry.is_static_type(AugmentedFooType) is False
    assert type_registry.is_static_type(FooType) is True


def test_has_code_defined_attribute(type_registry, static_types):
    attrs = {
        'id': Uuid(unique=True),
        'extra': String(unique=True),
        'cls_attr': "placeholder"
    }

    # test purely dynamic type
    NewType = type_registry.create_type("NewType", (Entity,), attrs)
    assert not type_registry.has_code_defined_attribute(NewType, "extra")
    assert not type_registry.has_code_defined_attribute(NewType, "cls_attr")
    assert not type_registry.has_code_defined_attribute(NewType, "nonexistent")

    # test purely code-defined type
    BarType = static_types['BarType']

    assert type_registry.has_code_defined_attribute(BarType, "extra")
    assert type_registry.has_code_defined_attribute(BarType, "cls_attr")
    assert not type_registry.has_code_defined_attribute(BarType, "nonexistent")

    # augment FooType with an "extra" attr; redefine "id" and "cls_attr" attrs
    FooType = static_types['FooType']
    type_registry.create_type("FooType", (Entity,), attrs)

    # test augmented type
    assert type_registry.has_code_defined_attribute(FooType, "id")
    assert not type_registry.has_code_defined_attribute(FooType, "extra")
    assert type_registry.has_code_defined_attribute(FooType, "cls_attr")
    assert not type_registry.has_code_defined_attribute(FooType, "nonexistent")

    # create static type with a dynamic subclass
    BazType = static_types['BazType']

    attrs = {'special': Bool(), 'extra': Bool(), 'cls_attr': "placeholder"}
    SubBazType = type_registry.create_type("SubBazType", (BazType,), attrs)

    # test the static type
    assert type_registry.has_code_defined_attribute(BazType, "name")
    assert type_registry.has_code_defined_attribute(BazType, "special")
    assert type_registry.has_code_defined_attribute(BazType, "cls_attr")

    # test the subtype does not override it
    assert type_registry.has_code_defined_attribute(SubBazType, "name")
    assert type_registry.has_code_defined_attribute(SubBazType, "special")
    assert type_registry.has_code_defined_attribute(SubBazType, "cls_attr")
    assert not type_registry.has_code_defined_attribute(SubBazType, "extra")

    # test augmented type after reload
    AType = static_types['AType']
    BType = static_types['BType']

    # augment the types
    # A1 and B1 are the new types A and B would deserialize to
    # after reloading the type hierarchy
    A1Type = type_registry.create_type("AType", (Entity,), {'bar': True})
    B1Type = type_registry.create_type("BType", (Entity,), {'bar': True})

    assert type_registry.has_code_defined_attribute(AType, "foo")
    assert type_registry.has_code_defined_attribute(BType, "foo")
    assert type_registry.has_code_defined_attribute(A1Type, "foo")
    assert type_registry.has_code_defined_attribute(B1Type, "foo")

    assert not type_registry.has_code_defined_attribute(AType, "bar")
    assert not type_registry.has_code_defined_attribute(BType, "bar")
    assert not type_registry.has_code_defined_attribute(A1Type, "bar")
    assert not type_registry.has_code_defined_attribute(B1Type, "bar")
