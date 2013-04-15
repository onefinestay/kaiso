from inspect import getmembers

from orp.attributes import Attribute

_descriptors = {}


def get_descriptor(cls):
    return _descriptors[cls.__name__]


def get_named_descriptor(name):
    return _descriptors[name]


def register_type(cls):
    descriptor = Descriptor(cls)
    _descriptors[cls.__name__] = descriptor
    return cls


def is_attribute(obj):
    return isinstance(obj, Attribute)


def get_index_name(cls):
    return cls.__name__.lower()


class Descriptor(object):
    # MJB: This needs a docstring.
    # MJB: It's slightly confusing that ``cls`` here (meaning Python class?)
    # MJB: can be a Type or an Instance in the taxonomy.
    def __init__(self, cls):
        self.cls = cls
        self.type_name = cls.__name__

        members = getmembers(cls, is_attribute)
        self.members = dict(members)

    def get_index_name_for_attribute(self, attr_name):
        # MJB: Docstring required here too
        # MJB: Is this conditional false when we're dealing with an Instance
        # MJB: class? I think we should make it explicit in comments that
        # MJB: isinstance(x, type) is only for for taxonomy Type classes
        if isinstance(self.cls, type):
            return get_index_name(self.cls)
        else:
            attr = self.members[attr_name]
            return get_index_name(attr.declared_on)

