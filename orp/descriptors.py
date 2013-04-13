from inspect import getmembers

from orp.attributes import Attribute


_descriptors = {}
_named_descriptors = {}


def get_descriptor(cls):
    return _descriptors[cls]


def get_named_descriptor(name):
    return _named_descriptors[name]


def register_type(cls):
    descriptor = Descriptor(cls)
    _descriptors[cls] = descriptor
    _named_descriptors[cls.__name__] = descriptor
    return cls


def is_attribute(obj):
    return isinstance(obj, Attribute)


def get_index_name(cls):
    return cls.__name__.lower()


class Descriptor(object):

    def __init__(self, cls):
        self.cls = cls
        self.type_name = cls.__name__

        members = getmembers(cls, is_attribute)
        self.members = dict(members)

    def get_index_name_for_attribute(self, attr_name):
        if isinstance(self.cls, type):
            return get_index_name(self.cls)
        else:
            attr = self.members[attr_name]
            return get_index_name(attr.declared_on)


