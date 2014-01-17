"""
Helpers for altering the type hierarchy.

"""

from collections import OrderedDict

from kaiso.types import TypeRegistry


def ensure_subclasses_remain_consistent(manager, type_id, new_bases):
    # ensure type_id exists
    registry = manager.type_registry
    cls = registry.get_class_by_id(type_id)

    # and is not static
    if registry.is_static_type(cls):
        raise ValueError("Cannot reparent static classes")  # TODO or can we?

    # ensure all new bases exist
    [registry.get_class_by_id(base) for base in new_bases]

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
        # we want to switch out the bases in the entry for `type_id`,
        # but all bases may not have been seen yet. if so, we defer
        # returning `type_id` (and any subclasses, or any other entries
        # that are also "waiting for" parent classes to appear

        # if we had transactions, we could start a transaction, change the
        # ISA relationships, call get_type_hierarchy() and then roll back
        # instead of this

        awaited_bases = set(new_bases)
        awaited_bases.add(type_id)

        new_type_inserted = False
        deferred_types = OrderedDict()
        deferred_types[type_id] = tuple(new_bases)

        for test_type_id, test_bases, _ in manager.get_type_hierarchy():
            if set.intersection(
                set(test_bases),
                set(deferred_types),
            ):
                deferred_types[test_type_id] = test_bases
                continue

            if test_type_id != type_id:
                yield (test_type_id, test_bases)

            awaited_bases.discard(test_type_id)

            if not new_type_inserted and not awaited_bases:
                new_type_inserted = True
                while deferred_types:
                    yield deferred_types.popitem(last=False)

        if awaited_bases:
            raise ValueError("One of the bases causes an inheritance cycle")

    temp_type_registry = TypeRegistry()
    try:
        for test_type_id, bases in new_type_hierarchy(manager):
            bases = tuple(
                temp_type_registry.get_class_by_id(base) for base in bases
            )
            temp_type_registry.create_type(str(test_type_id), bases, {})
    except TypeError as ex:
        # bad mro
        raise ValueError("Invalid mro for {} ({})".format(test_type_id, ex))
