from inspect import getmembers, getmro


_descriptors = {}


def get_declaring_class(cls, attr_name):
    """ Returns the class in the type heirarchhy of ``cls`` defined attribute
    ``attr_name``. """
    for base in getmro(cls):
        if hasattr(base, attr_name):
            return base
    raise AttributeError(
        "Attribute not defined on any base class of {}".format(cls)
    )


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
                declaring_class = get_declaring_class(descr.cls, name)

                index_name = get_index_name(declaring_class)
                key = name
                value = attr.to_db(getattr(obj, name))
                yield (index_name, key, value)


class Descriptor(object):
    """ Provides information about the types of persistable objects.

    It's main purpose is to provide type names and attributes information of
    persistable types(classes).
    """
    def __init__(self, cls):
        self.cls = cls
        self.type_name = cls.__name__

        self._members = None
        self._relationships = None

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
            return get_index_name(get_declaring_class(self.cls, attr_name))

    @property
    def relationships(self):
        from kaiso.attributes.bases import _is_relationship_reference
        relationships = self._relationships
        if relationships is None:
            relationships = dict(getmembers(
                self.cls, _is_relationship_reference
            ))
            self._relationships = relationships

        return relationships

    @property
    def members(self):
        members = self._members
        if members is None:
            members = dict(getmembers(self.cls, _is_attribute))
            self._members = members

        return members


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


register_type(type)
register_type(object)


class Persistable(object):
    ''' The base of all persistable objects.

    Any object stored in the db must inherit from this class.
    Any object having Persistable as it's base are considered persistable.
    '''


@register_type
class PersistableMeta(type, Persistable):
    def __new__(mcs, name, bases, dct):
        cls = super(PersistableMeta, mcs).__new__(mcs, name, bases, dct)
        register_type(cls)
        return cls

    def __init__(cls, name, bases, dct):
        super(PersistableMeta, cls).__init__(name, bases, dct)


def _is_attribute(obj):
    return isinstance(obj, Attribute)


class Attribute(Persistable):
    __metaclass__ = PersistableMeta

    def __init__(self, unique=False):
        self.unique = unique

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_db(value):
        return value


class AttributedBase(Persistable):
    """ The base class for objects that can have Attributes.

    Sets default values during instance creation and applies kwargs
    passed to __init__.
    """
    __metaclass__ = PersistableMeta

    def __new__(cls, *args, **kwargs):

        obj = super(AttributedBase, cls).__new__(cls, *args, **kwargs)

        # setup default values for attributes
        descriptor = get_descriptor(cls)

        # TODO: rename members->attributes?
        for name, attr in descriptor.members.items():
            setattr(obj, name, attr.default)

        return obj

    def __init__(self, *args, **kwargs):
        super(AttributedBase, self).__init__(*args, **kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)


class Entity(AttributedBase):
    def __getattribute__(self, name):
        cls = type(self)
        descriptor = get_descriptor(cls)
        try:
            rel_reference = descriptor.relationships[name]
        except KeyError:
            return object.__getattribute__(self, name)
        else:
            return rel_reference.get_manager(self)


class Relationship(AttributedBase):
    def __init__(self, start, end, **kwargs):
        super(Relationship, self).__init__(**kwargs)

        self.start = start
        self.end = end




class DefaultableAttribute(Attribute):
    # TODO: should it live in types.py?

    def __init__(self, default=None, unique=False):
        super(DefaultableAttribute, self).__init__(unique)
        self.default = default

