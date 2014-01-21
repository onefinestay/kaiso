"""
Helpers for altering the type hierarchy.

"""

from collections import OrderedDict

from kaiso.types import TypeRegistry


def validate_base_change(manager, type_id, new_bases):
    """
    Make sure it would be ok to change the bases of `type_id` to `new_bases`

    If this would result in an inconsistent type hieararchy,
    raise `ValueError`. Otherwise, return None.


    In `manager.get_type_hierarchy`, `type_id` is guaranteed to appear after
    all its base classes, and before all its subclasses.

    To remain consistent, all new_bases must appear before any subclasses
    of type_id

    We make a new list of (type_id, bases) pairs by
    1) removing the old entry for (type_id, (...))
    2) adding a new entry (type_id, new_bases) once we've seen each
       entry in new_bases

    We also discard attributes since we don't care about them for this.

    Finally, we attempt to create the types in this new hierarchy,
    which will throw `TypeError` if we can't maintain consistency.
    """

    # ensure type_id exists
    registry = manager.type_registry

    # ensure type and all new bases exist (this raises UnknownType otherwise)
    registry.get_class_by_id(type_id)
    [registry.get_class_by_id(base) for base in new_bases]

    def new_type_hierarchy(manager):
        """
        We want to switch out the bases in the entry for `type_id`,
        but all bases may not have been seen yet. If so, we defer
        returning `type_id` (and any subclasses, or any other entries
        that are also "waiting for" parent classes to appear.

        If we had transactions, we could start a transaction, change the
        ISA relationships, call get_type_hierarchy() and then roll back
        instead of this.
        """

        awaited_bases = set(new_bases)
        awaited_bases.add(type_id)

        new_type_inserted = False
        deferred_types = OrderedDict()
        deferred_types[type_id] = tuple(new_bases)

        for test_type_id, test_bases, _ in manager.get_type_hierarchy():
            if set(test_bases).intersection(deferred_types):
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
    for test_type_id, bases in new_type_hierarchy(manager):
        bases = tuple(
            temp_type_registry.get_class_by_id(base) for base in bases
        )
        type_name = str(test_type_id)
        try:
            temp_type_registry.create_type(type_name, bases, {})
        except TypeError as ex:
            # bad mro
            raise ValueError(
                "Invalid mro for {} ({})".format(test_type_id, ex)
            )
