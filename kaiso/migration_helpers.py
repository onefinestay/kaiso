"""
Helpers for altering the type hierarchy.

"""

from collections import OrderedDict

from kaiso.types import TypeRegistry


def get_type_registry_with_base_change(manager, type_id, new_bases):
    """
    Returns an amended type-registry with the bases for the given `type_id`
    set to the given `new_bases`.
    Useful for making sure it would be ok to change the bases of `type_id` to
    `new_bases`.

    If the base change results in an inconsistent type hieararchy,
    `ValueError` is raised. Otherwise, a type_registry object containing the
    amended type is returned.

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

    # capture current attrs of the type being amended
    type_attrs = registry.get_descriptor_by_id(type_id).declared_attributes

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
        deferred_types[type_id] = (tuple(new_bases), type_attrs)

        current_hierarchy = manager.get_type_hierarchy()

        for test_type_id, test_bases, test_attrs in current_hierarchy:
            if set(test_bases).intersection(deferred_types):
                deferred_types[test_type_id] = (test_bases, test_attrs)
                continue

            if test_type_id != type_id:
                yield (test_type_id, test_bases, test_attrs)

            awaited_bases.discard(test_type_id)

            if not new_type_inserted and not awaited_bases:
                new_type_inserted = True
                while deferred_types:
                    dfr_type, (dfr_bases, dfr_attrs) = deferred_types.popitem(
                        last=False)
                    yield (dfr_type, dfr_bases, dfr_attrs)

        if awaited_bases:
            raise ValueError("One of the bases causes an inheritance cycle")

    amended_type_registry = TypeRegistry()
    for test_type_id, bases, attrs in new_type_hierarchy(manager):
        bases = tuple(
            amended_type_registry.get_class_by_id(base) for base in bases
        )
        type_name = str(test_type_id)
        try:
            amended_type_registry.create_type(type_name, bases, attrs)
        except TypeError as ex:
            # bad mro
            raise ValueError(
                "Invalid mro for {} ({})".format(test_type_id, ex)
            )
    return amended_type_registry
