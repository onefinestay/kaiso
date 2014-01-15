import pytest

from kaiso.migration_helpers import ensure_subclasses_remain_consistent
from kaiso.types import collector, Entity


def test_basic(manager):
    with collector() as collected:
        class A(Entity): pass
        class A2(A): pass

        class B(Entity): pass
        class B2(B): pass

        class AB(A, B): pass

    manager.save_collected_classes(collected)
    ensure_subclasses_remain_consistent(manager, 'AB', ('A2', 'B2'))


def test_become_your_own_ancestor(manager):
    with collector() as collected:
        class A(Entity): pass
        class A2(A): pass
        class A3(A2): pass

    manager.save_collected_classes(collected)

    # become your own parent
    with pytest.raises(ValueError) as ex:
        ensure_subclasses_remain_consistent(manager, 'A', ('A3',))
    # TODO: check ex


def test_duplicate_base_class(manager):
    with collector() as collected:
        class A(Entity): pass
        class B(Entity): pass
        class C(A, B): pass

    manager.save_collected_classes(collected)

    with pytest.raises(ValueError) as ex:
        ensure_subclasses_remain_consistent(manager, 'C', ('A', 'B', 'A'))
    assert "duplicate base class" in str(ex)


def test_move_down_the_hieararchy(manager):

    with collector() as collected:
        class A(Entity): pass
        class A2(A): pass
        class A3(A2): pass
        class A4(A3): pass

        class B(A): pass
        class C(B): pass

    manager.save_collected_classes(collected)

    ensure_subclasses_remain_consistent(manager, 'B', ('A4',))


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
        class X(Entity): pass
        class Y(Entity): pass

        class A(X, Y): pass
        class B(Y): pass # to become B(Y, X)

        class AB(A, B): pass

    manager.save_collected_classes(collected)

    with pytest.raises(ValueError) as ex:
        ensure_subclasses_remain_consistent(manager, 'B', ('Y', 'X'))
    assert "Cannot create a consistent method resolution" in str(ex)
