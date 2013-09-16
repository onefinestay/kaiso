import pytest

from kaiso.attributes import String, Bool
from kaiso.exceptions import CannotUpdateType, UnsupportedTypeError
from kaiso.persistence import get_type_id
from kaiso.types import Entity, collector


@pytest.fixture
def static_types(manager):
    class Animal(Entity):
        name = String()

    class Mammal(Animal):
        pass

    class WaterBound(Animal):
        freshwater = Bool()

    class Cetacean(Mammal):
        pass

    manager.save(Animal)
    manager.save(Mammal)
    manager.save(WaterBound)
    manager.save(Cetacean)

    return {
        'Animal': Animal,
        'Mammal': Mammal,
        'WaterBound': WaterBound,
        'Cetacean': Cetacean,
    }


def test_cannot_reparent_non_type(manager, static_types):
    Animal = static_types['Animal']

    animal = Animal()

    with pytest.raises(UnsupportedTypeError) as e:
        manager.update_type(animal, (Animal,))
    assert e.value.message == "Object is not a PersistableType"


def test_cannot_reparent_code_defined_types(manager, static_types):
    Animal = static_types['Animal']
    Cetacean = static_types['Cetacean']

    manager.save(Cetacean)

    with pytest.raises(CannotUpdateType) as e:
        manager.update_type(Cetacean, (Animal,))
    assert "defined in code" in e.value.message


def test_cannot_reparent_with_incompatible_attributes(manager, static_types):
    Cetacean = static_types['Cetacean']
    WaterBound = static_types['WaterBound']

    Whale = manager.create_type('Whale', (WaterBound,), {})
    manager.save(Whale)

    with pytest.raises(CannotUpdateType) as e:
        manager.update_type(Whale, (Cetacean,))
    assert e.value.message == "Inherited attributes are not identical"

    with collector() as collected:
        class A(Entity):
            foo = String()

        class B(Entity):
            foo = Bool()
    manager.save_collected_classes(collected)

    C = manager.create_type('C', (A,), {})
    manager.save(C)

    with pytest.raises(CannotUpdateType) as e:
        manager.update_type(C, (B,))
    assert e.value.message == "Inherited attributes are not identical"


def test_reparent(manager, static_types):
    """
        (Mammal) <-[:ISA]- (Whale)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Whale)
    """
    Cetacean = static_types['Cetacean']
    Mammal = static_types['Mammal']

    Whale = manager.create_type('Whale', (Mammal,), {})
    manager.save(Whale)

    manager.save(Cetacean)
    manager.update_type(Whale, (Cetacean,))

    UpdatedWhale = manager.type_registry.get_class_by_id(get_type_id(Whale))
    assert UpdatedWhale.__bases__ == (Cetacean,)
    assert issubclass(UpdatedWhale, Cetacean)


def test_reparent_with_subtypes(manager, static_types):
    """
        (Mammal) <-[:ISA]- (Whale) <-[:ISA]- (Orca)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Whale) <-[:ISA]- (Orca)
    """
    Cetacean = static_types['Cetacean']
    Mammal = static_types['Mammal']

    Whale = manager.create_type('Whale', (Mammal,), {})
    Orca = manager.create_type('Orca', (Whale,), {})
    manager.save(Orca)

    manager.save(Cetacean)
    manager.update_type(Whale, (Cetacean,))

    UpdatedWhale = manager.type_registry.get_class_by_id(get_type_id(Whale))
    UpdatedOrca = manager.type_registry.get_class_by_id(get_type_id(Orca))
    assert UpdatedOrca.__bases__ == (UpdatedWhale,)
    assert issubclass(UpdatedOrca, Cetacean)


def test_reparent_with_declared_attributes(manager, static_types):
    """
        (Mammal) <-[:ISA]- (Whale) <-[:ISA]- (Orca) <-[:DECLAREDON]- (name)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Whale) <-[:ISA]- (Orca)
                                                                     ^
                                              (name) -[:DECLAREDON] -'
    """
    Cetacean = static_types['Cetacean']
    Mammal = static_types['Mammal']

    Whale = manager.create_type('Whale', (Mammal,), {})
    Orca = manager.create_type('Orca', (Whale,), {'name': String()})
    manager.save(Orca)

    manager.save(Cetacean)
    manager.update_type(Whale, (Cetacean,))

    UpdatedWhale = manager.type_registry.get_class_by_id(get_type_id(Whale))
    UpdatedOrca = manager.type_registry.get_class_by_id(get_type_id(Orca))
    assert UpdatedOrca.__bases__ == (UpdatedWhale,)
    assert issubclass(UpdatedOrca, Cetacean)

    descriptor = manager.type_registry.get_descriptor(UpdatedOrca)
    assert "name" in descriptor.declared_attributes


def test_reparent_with_matching_attributes(manager):
    """ Reparenting with different but identical attributes is allowed.
    """
    with collector() as collected:
        class C(Entity):
            foo = String()

        class D(Entity):
            foo = String()
    manager.save_collected_classes(collected)

    E = manager.create_type('E', (C,), {})
    manager.save(E)

    manager.update_type(E, (D,))

    UpdatedD = manager.type_registry.get_class_by_id(get_type_id(D))
    UpdatedE = manager.type_registry.get_class_by_id(get_type_id(E))
    assert UpdatedE.__bases__ == (UpdatedD,)
    assert issubclass(UpdatedE, UpdatedD)


def test_reparent_with_instances(manager, static_types):
    """
        (Mammal) <-[:ISA]- (Whale) <-[:ISA]- (Orca) <-[:INSTANCEOF]- (willy)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Orca) <-[:INSTANCEOF]- (willy)
    """
    Cetacean = static_types['Cetacean']
    Mammal = static_types['Mammal']

    Whale = manager.create_type('Whale', (Mammal,), {})
    Orca = manager.create_type('Orca', (Whale,), {})
    willy = Orca(name="willy")

    manager.save(Orca)
    manager.save(willy)

    manager.save(Cetacean)
    manager.update_type(Orca, (Cetacean,))

    UpdatedOrca = manager.type_registry.get_class_by_id(get_type_id(Orca))
    assert UpdatedOrca.__bases__ == (Cetacean,)
    assert issubclass(UpdatedOrca, Cetacean)

    updated_willy = manager.get(Orca, name="willy")
    assert updated_willy.__class__ == UpdatedOrca
    assert isinstance(updated_willy, Cetacean)


def test_reparent_multiple_inheritance(manager):
    """
        (Noun) <-[:ISA]-                      (Verb) <-[:ISA]-
                         \                                     \
                          (Mop)    becomes:                     (Mop)
                         /                                     /
    (Physical) <-[:ISA]-                    (Action) <-[:ISA]-
    """
    Physical = type("Pysical", (Entity,), {})
    Action = type("Action", (Entity,), {})
    Word = type("Word", (Entity,), {})
    Noun = type("Noun", (Word,), {})
    Verb = type("Verb", (Word,), {})

    Mop = manager.create_type('Mop', (Physical, Noun), {})
    manager.save(Mop)

    manager.save(Action)
    manager.save(Verb)
    manager.update_type(Mop, (Action, Verb,))

    UpdatedMop = manager.type_registry.get_class_by_id(get_type_id(Mop))
    assert UpdatedMop.__bases__ == (Action, Verb,)
    assert issubclass(UpdatedMop, Action)
    assert issubclass(UpdatedMop, Verb)
    assert not issubclass(UpdatedMop, (Physical, Noun))


def test_reparent_diamond(manager):
    """
                 -[:ISA]- (Flavouring) <-[:ISA]-
               /                                \
    (Edible) <-                                  - (Beetroot) <-[:ISA]-
               \                               /                       |
                 -[:ISA]- (Colouring) <-[:ISA]-                   (GoldenBeet)
    becomes:
    (Edible) <-[:ISA]- (Vegetable) <-[:ISA]- (Beetroot) <-[:ISA]- (GoldenBeet)
    """
    Edible = type("Edible", (Entity,), {})
    XFlavouring = type("XFlavouring", (Edible,), {})
    XColouring = type("XColouring", (Edible,), {})

    XBeetroot = manager.create_type("XBeetroot", (XFlavouring, XColouring), {})
    GoldenBeet = manager.create_type("GoldenBeet", (XBeetroot,), {})

    Vegetable = type("Vegetable", (Edible,), {})

    manager.save(GoldenBeet)
    manager.save(Vegetable)
    manager.update_type(XBeetroot, (Vegetable,))

    UpdatedBeetroot = manager.type_registry.get_class_by_id(
        get_type_id(XBeetroot))
    UpdatedGoldenBeet = manager.type_registry.get_class_by_id(
        get_type_id(GoldenBeet))
    assert issubclass(UpdatedBeetroot, Vegetable)
    assert issubclass(UpdatedGoldenBeet, UpdatedBeetroot)
    assert not issubclass(UpdatedBeetroot, (XFlavouring, XFlavouring))
    assert not issubclass(UpdatedGoldenBeet, (XFlavouring, XFlavouring))


def test_reparent_missing_type(manager, static_types):
    Cetacean = static_types['Cetacean']
    Mammal = static_types['Mammal']

    Whale = manager.create_type('Whale', (Mammal,), {})

    manager.save(Cetacean)
    with pytest.raises(CannotUpdateType) as e:
        manager.update_type(Whale, (Cetacean,))
    assert e.value.message == "Type or bases not found in the database."


def test_reparent_to_missing_type(manager):
    class Animal(Entity):
        name = String()

    class Mammal(Animal):
        pass

    class Cetacean(Mammal):
        pass

    Whale = manager.create_type('Whale', (Mammal,), {})
    manager.save(Whale)

    with pytest.raises(CannotUpdateType) as e:
        manager.update_type(Whale, (Cetacean,))
    assert e.value.message == "Type or bases not found in the database."
