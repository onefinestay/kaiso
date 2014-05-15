import pytest

from kaiso.exceptions import UnknownType
from kaiso.migration_helpers import get_type_registry_with_base_change
from kaiso.types import collector, Entity
from kaiso.attributes import Integer, String


def test_basic(manager):
    with collector() as collected:
        class A(Entity):
            pass

        class A2(A):
            pass

        class B(Entity):
            pass

        class B2(B):
            pass

        class AB(A, B):
            pass

    manager.save_collected_classes(collected)
    amended_registry = get_type_registry_with_base_change(
        manager, 'AB', ('A2', 'B2'))

    AmendedAB = amended_registry.get_class_by_id('AB')
    assert [c.__name__ for c in AmendedAB.mro()] == [
        'AB', 'A2', 'A', 'B2', 'B', 'Entity', 'AttributedBase',
        'Persistable', 'object'
    ]

    # check original registry is unchanged
    OriginalAB = manager.type_registry.get_class_by_id('AB')
    assert [c.__name__ for c in OriginalAB.mro()] == [
        'AB', 'A', 'B', 'Entity', 'AttributedBase',
        'Persistable', 'object'
    ]


def test_become_your_own_ancestor(manager):
    with collector() as collected:
        class A(Entity):
            pass

        class A2(A):
            pass

        class A3(A2):
            pass

    manager.save_collected_classes(collected)

    # become your own parent
    with pytest.raises(ValueError) as ex:
        get_type_registry_with_base_change(manager, 'A', ('A3',))
    assert "inheritance cycle" in str(ex)


def test_duplicate_base_class(manager):
    with collector() as collected:
        class A(Entity):
            pass

        class B(Entity):
            pass

        class C(A, B):
            pass

    manager.save_collected_classes(collected)

    with pytest.raises(ValueError) as ex:
        get_type_registry_with_base_change(manager, 'C', ('A', 'B', 'A'))
    assert "duplicate base class" in str(ex)


def test_move_down_the_hieararchy(manager):

    with collector() as collected:
        class A(Entity):
            pass

        class A2(A):
            pass

        class A3(A2):
            pass

        class A4(A3):
            pass

        class B(A):
            pass

        class C(B):
            pass

    manager.save_collected_classes(collected)

    get_type_registry_with_base_change(manager, 'B', ('A4',))


def test_bad_mro(manager):
    """
    from http://www.python.org/download/releases/2.3/mro/

     -----------
    |           |
    |    O      |
    |  /   \    |
     - X    Y  /
       |  / | /
       | /  |/
       A    B
       \   /
         ?
    """

    with collector() as collected:
        class X(Entity):
            pass

        class Y(Entity):
            pass

        class A(X, Y):
            pass

        class B(Y):  # to become B(Y, X)
            pass

        class AB(A, B):
            pass

    manager.save_collected_classes(collected)

    with pytest.raises(ValueError) as ex:
        get_type_registry_with_base_change(manager, 'B', ('Y', 'X'))
    assert "Cannot create a consistent method resolution" in str(ex)


def test_unknown_types(manager):
    with collector() as collected:
        class A(Entity):
            pass

    manager.save_collected_classes(collected)

    with pytest.raises(UnknownType):
        get_type_registry_with_base_change(manager, 'A', ['Invalid'])

    with pytest.raises(UnknownType):
        get_type_registry_with_base_change(manager, 'Invalid', ['A'])


def test_amended_indexes(manager):
    with collector() as collected:
        class A(Entity):
            id = Integer(unique=True)

        class B(Entity):
            code = String(unique=True)

    manager.save_collected_classes(collected)

    amended_registry = get_type_registry_with_base_change(
        manager, 'B', ('A',))

    # confirm that the "amended" indexes of B are now using both id and code
    amended_indexes = amended_registry.get_indexes_for_type(B)
    assert {(ind, key) for ind, key, _ in amended_indexes} == {
        ('a', 'id'), ('b', 'code')
    }


def test_amended_indexes_same_attr_name(manager):
    with collector() as collected:
        class A(Entity):
            id = Integer(unique=True)

        class B(Entity):
            id = String(unique=True)

        class C(A):
            pass

    manager.save_collected_classes(collected)

    amended_registry = get_type_registry_with_base_change(
        manager, 'C', ('A', 'B'))

    # confirm that the "amended" indexes of C are still just A.id
    amended_indexes = amended_registry.get_indexes_for_type(C)
    assert {(ind, key) for ind, key, _ in amended_indexes} == {
        ('a', 'id'),
    }
