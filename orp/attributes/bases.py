from orp.references import get_store_for_object


class RelationshipReference(object):
    def __init__(self, relationship_class, min, max):
        self.relationship_class = relationship_class
        self.min = min
        self.max = max

    def get(self, instance):
        store = get_store_for_object(instance)

        objects = store.get_related_objects(
            self.relationship_class, type(self), instance)

        objects = (obj for (obj, ) in objects)

        if self.max <= 1:
            return next(objects, None)
        else:
            return objects


class Attribute(object):
    def __init__(self, unique=False):
        self.unique = unique

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_db(value):
        return value


class DefaultableAttribute(Attribute):
    def __init__(self, default=None, unique=False):
        # do we have a preference for using super() over <BaseClass>.<method>?
        super(DefaultableAttribute, self).__init__(unique)
        self.default = default
