"""
Helpers for altering the type hierarchy.

"""

from collections import OrderedDict

from kaiso.exceptions import  UnknownType
from kaiso.types import TypeRegistry


def ensure_subclasses_remain_consistent(manager, type_id, new_bases):
    # ensure type_id exists
    registry = manager.type_registry
    cls = registry.get_class_by_id(type_id)

    # and is not static
    if registry.is_static_type(cls):
        raise ValueError("Cannot reparent static classes")  # TODO or can we?


    # type_id is guaranteed to appear after all its base classes, and before
    # all its subclasses

    # to remain consistent, all new_bases must appear before any subclasses
    # of type_id

    # we make a new list of (type_id, bases) pairs by
    # 1) removing the old entry for (type_id, (...))
    # 2) adding a new entry (type_id, new_bases) once we've seen each
    #    entry in new_bases

    # we also discard attributes since we don't care about them for this

    # finally, we attempt to create the types in this new hierarchy,
    # which will throw TypeError if we can't maintain consistency

    def new_type_hierarchy(manager):
        bases = tuple(new_bases)
        waiting_for = set(new_bases)
        waiting_for.add(type_id)

        # until type_id has been re-inserted, try to defer any dependencies
        new_type_inserted = False
        defer = OrderedDict()
        defer[type_id] = bases

        for test_type_id, test_bases, _ in manager.get_type_hierarchy():
            if set.intersection(
                set(test_bases),
                set(defer),
            ):
                defer[test_type_id] =  test_bases
                continue

            if test_type_id != type_id:
                yield (test_type_id, test_bases)

            waiting_for.discard(test_type_id)

            if not new_type_inserted and not waiting_for:
                new_type_inserted = True
                while defer:
                    yield defer.popitem(last=False)

        if waiting_for:
            raise ValueError("One of the bases causes an inheritance cycle")

    temp_type_registry = TypeRegistry()
    try:
        for test_type_id, bases in new_type_hierarchy(manager):
            bases = tuple(
                temp_type_registry.get_class_by_id(base) for base in bases
            )
            temp_type_registry.create_type(str(test_type_id), bases, {})
    # except IOError: pass
    except TypeError as ex:
        # bad mro
        raise ValueError("Invalid mro for {} ({})".format(test_type_id, ex))
    except UnknownType as ex:
        # child appears before parent
        raise ValueError("Invalid mro for {}: ({})".format(test_type_id, ex))
