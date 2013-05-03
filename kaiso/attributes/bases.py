from kaiso.exceptions import MultipleObjectsFound, NoResultFound
from kaiso.references import get_store_for_object

_attribute_types = {}


def wraps_type(cls):
    def wrapper(attr_cls):
        _attribute_types[cls] = attr_cls
        return attr_cls

    return wrapper


def get_attibute_for_type(cls):
    return _attribute_types[cls]


class RelationshipManager(object):
    def __init__(self, obj, relationship_reference):
        self.obj = obj
        self.relationship_reference = relationship_reference

    def _related_objects(self):
        obj = self.obj
        relationship_reference = self.relationship_reference

        store = get_store_for_object(obj)

        related_objects = store.get_related_objects(
            relationship_reference.relationship_class,
            type(relationship_reference),
            obj)

        return related_objects

    def __iter__(self):
        return (rel_obj for rel_obj, _ in self._related_objects())

    @property
    def relationships(self):
        return (rel for _, rel in self._related_objects())

    def first(self):
        return next(iter(self), None)

    def one(self):
        related_objects = iter(self)
        first = next(related_objects, None)
        second = next(related_objects, None)

        if second is not None:
            raise MultipleObjectsFound

        if first is None:
            raise NoResultFound

        return first


def _is_relationship_reference(obj):
    return isinstance(obj, RelationshipReference)


class RelationshipReference(object):
    def __init__(self, relationship_class):
        self.relationship_class = relationship_class

    def get_manager(self, obj):
        return RelationshipManager(obj, self)
