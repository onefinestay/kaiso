""" Provides functions and classes to get information about persistable
objects and their types.


The functions are mainly used by the persistence module and provide:

* information about attributes declared on class
* indexes for an object, their index names, keys and values
* handling of class registration

"""
from inspect import getmembers

from kaiso.attributes import Attribute, RelationshipReference


_descriptors = {}


def get_descriptor(cls):
    """ Returns a descriptor for a registered class.

    Args:
        cls: A registered class.

    Returns:
        A Descriptor object.
    """
    return _descriptors[cls.__name__]


def get_descriptor_by_name(name):
    """ Returns a descriptor given it's registered name.

    Args:
        name: The registered name of a class.

    Returns:
        A Descriptor object.
    """
    return _descriptors[name]


def register_type(cls):
    """ Registers a class for type introspection.

    This function can be used as a class decorator to register a class.

    Args:
        A class to be registered.

    Returns:
        cls
    """
    descriptor = Descriptor(cls)
    _descriptors[cls.__name__] = descriptor
    return cls


def get_index_name(cls):
    """ Returns a cypher index name for a class.

    Args:
        cls: The class to generate an index for.

    Returns:
        An index name.
    """
    return cls.__name__.lower()


def get_indexes(obj):
    """ Returns indexes for a persistable object.

    Args:
        obj: A persistable object.

    Returns:
        Generator yielding tuples (index_name, key, value)
    """

    obj_type = type(obj)

    if isinstance(obj, type):
        index_name = get_index_name(obj_type)
        value = get_descriptor(obj).type_name
        yield (index_name, 'name', value)
    else:
        descr = get_descriptor(obj_type)

        for name, attr in descr.members.items():
            if attr.unique:
                index_name = get_index_name(attr.declared_on)
                key = name
                value = attr.to_db(getattr(obj, name))
                yield (index_name, key, value)


def _is_attribute(obj):
    return isinstance(obj, Attribute)


def _is_relationship_reference(obj):
    return isinstance(obj, RelationshipReference)


class Descriptor(object):
    """ Provides information about the types of persistable objects.

    It's main purpose is to provide type names and attributes information of
    persistable types(classes).
    """
    def __init__(self, cls):
        self.cls = cls
        self.type_name = cls.__name__

        members = getmembers(cls, _is_attribute)
        relationships = getmembers(cls, _is_relationship_reference)

        self.members = dict(members)
        self.relationships = dict(relationships)

    def get_index_name_for_attribute(self, attr_name=None):
        """ Returns the index name for the attribute declared on
        the descriptor's class or it's bases.

        If the descriptor's class is a subclass of <type>, i.e. a meta-class,
        the attr_name arg is not required.

        Otherwise, the index will be based on the class on which the attribute,
        identified by attr_name was declared on.

        Args:
            attr_name: The name of the attribute for which to return an index.

        Returns:
            The index name, which can be used in index lookups in cypher.
        """
        if issubclass(self.cls, type):
            # we are dealing with a meta-class
            return get_index_name(self.cls)
        else:
            attr = self.members[attr_name]
            return get_index_name(attr.declared_on)
