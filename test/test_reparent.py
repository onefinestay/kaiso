import pytest

from kaiso.attributes import String
from kaiso.exceptions import CannotUpdateType
from kaiso.types import Entity
from kaiso.persistence import get_type_id


class Animal(Entity):
    name = String()


class Mammal(Animal):
    pass


class Cetacean(Mammal):
    pass


def test_cannot_reparent_non_type(manager):

    animal = Animal()

    with pytest.raises(RuntimeError):
        manager.update_type(animal, (Animal,))


def test_cannot_reparent_code_defined_types(manager):

    manager.save(Cetacean)

    with pytest.raises(CannotUpdateType):
        manager.update_type(Cetacean, (Animal,))


def test_cannot_reparent_types_with_attributes(manager):

    Whale = manager.create_type('Whale', (Cetacean,), {'name': String()})
    manager.save(Whale)

    with pytest.raises(CannotUpdateType):
        manager.update_type(Whale, (Cetacean,))


def test_reparent(manager):
    """
        (Mammal) <-[:ISA]- (Whale)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Whale)
    """
    Whale = manager.create_type('Whale', (Mammal,), {})
    manager.save(Whale)

    manager.save(Cetacean)
    manager.update_type(Whale, (Cetacean,))

    UpdatedWhale = manager.type_registry.get_class_by_id(get_type_id(Whale))
    assert UpdatedWhale.__bases__ == (Cetacean,)
    assert issubclass(UpdatedWhale, Cetacean)


def test_reparent_with_subtypes(manager):
    """
        (Mammal) <-[:ISA]- (Whale) <-[:ISA]- (Orca)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Whale) <-[:ISA]- (Orca)
    """
    Whale = manager.create_type('Whale', (Mammal,), {})
    Orca = manager.create_type('Orca', (Whale,), {})
    manager.save(Orca)

    manager.save(Cetacean)
    manager.update_type(Whale, (Cetacean,))

    UpdatedWhale = manager.type_registry.get_class_by_id(get_type_id(Whale))
    UpdatedOrca = manager.type_registry.get_class_by_id(get_type_id(Orca))
    assert UpdatedOrca.__bases__ == (UpdatedWhale,)
    assert issubclass(UpdatedOrca, Cetacean)


def test_reparent_with_instances(manager):
    """
        (Mammal) <-[:ISA]- (Whale) <-[:ISA]- (Orca) <-[:INSTANCEOF]- (willy)
    becomes:
        (Mammal) <-[:ISA]- (Cetacean) <-[:ISA]- (Orca) <-[:INSTANCEOF]- (willy)
    """
    Whale = manager.create_type('Whale', (Mammal,), {})
    Orca = manager.create_type('Orca', (Whale,), {})
    willy = Orca(name="willy")
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


def test_reparent_missing_type(manager):
    Whale = manager.create_type('Whale', (Mammal,), {})

    manager.save(Cetacean)
    with pytest.raises(CannotUpdateType):
        manager.update_type(Whale, (Cetacean,))


def test_reparent_to_missing_type(manager):
    Whale = manager.create_type('Whale', (Mammal,), {})
    manager.save(Whale)

    with pytest.raises(CannotUpdateType):
        manager.update_type(Whale, (Cetacean,))
