from inspect import getmembers

from orp.attributes import Attribute

_descriptors = {}


def get_descriptor(cls):
    ''' Returns a descriptor for a registered class.

    Args:
        cls: A registered class.

    Returns:
        A Descriptor object.
    '''
    return _descriptors[cls.__name__]


def get_named_descriptor(name):
    ''' Returns a descriptor given it's registered name.

    Args:
        name: The registered name of a class.

    Returns:
        A Descriptor object.
    '''
    return _descriptors[name]


def register_type(cls):
    ''' Registers a class for wich one can get a descriptor.

    This function can be used as a class decorator to register a class.

    Args:
        A class to be registered.

    Returns:
        cls
    '''
    descriptor = Descriptor(cls)
    _descriptors[cls.__name__] = descriptor
    return cls


def is_attribute(obj):
    ''' Returns if an obj is an Attribute.

    Args:
        obj: The object to test if it is an isinstance of Attribute.

    Returns:
        True if it is an Attribute instance,
        False otherwise.
    '''
    return isinstance(obj, Attribute)


def get_index_name(cls):
    ''' Returns a cypher index name for a class.

    Args:
        cls: The class to generate an index for.

    Returns:
        An index name.
    '''
    return cls.__name__.lower()


def get_indexes(obj):
    ''' Returns indexes for a persistable object.

    Args:
        obj: A persistable object.

    Returns:
        Tuples (index_name, key, value) which can be used to index an object.
    '''

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


class Descriptor(object):
    ''' Provides information about the types of persistable objects.

    If the type of the persistable object is a subclass of <type>,
    i.e. a meta-class, there will be only on index
    and that is the name of the class.

    '''
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
        if issubclass(self.cls, type):
            return get_index_name(self.cls)
        else:
            attr = self.members[attr_name]
            return get_index_name(attr.declared_on)

