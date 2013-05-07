from inspect import getmembers, getmro, isfunction, isdatadescriptor

from kaiso.exceptions import UnknownType, TypeAlreadyRegistered


def get_declaring_class(cls, attr_name):
    """ Returns the class in the type heirarchy of ``cls`` that defined
        an attribute with name ``attr_name``.
    """
    declaring_class = None
    declared_attr = None

    # Start at the top of the hierarchy and determine which of the MRO have
    # the attribute. Return the lowest class that defines (or overloads) the
    # attribute.
    for base in reversed(getmro(cls)):
        attr = getattr(base, attr_name, False)
        if attr and declared_attr is not attr:
            declaring_class = base
            declared_attr = attr

    return declaring_class


def get_index_name(cls):
    """ Returns a cypher index name for a class.

    Args:
        cls: The class to generate an index for.

    Returns:
        An index name.
    """
    if issubclass(cls, PersistableMeta):
        return cls.index_name
    else:
        return cls.__name__.lower()


def get_index_entries(obj):
    """ Returns tuples of (index-name, key, value) for a persistable object.

    It can be used to create queryies with index lookups.

    Args:
        obj: A persistable object.

    Returns:
        Generator yielding tuples (index_name, key, value)
    """

    cls = type(obj)

    if issubclass(cls, PersistableMeta):
        meta_cls = cls
    else:
        meta_cls = type(cls)

    return meta_cls.get_index_entries(obj)


def is_indexable(cls):
    """ Returns True if the class ``cls`` has any indexable attributes.
    """

    descr = Descriptor(cls)
    for _, attr in descr.attributes.items():
        if attr.unique:
            return True

    return False


class Descriptor(object):
    """ Provides information about the types of persistable objects.

    Its main purpose is to provide type names and attribute information of
    persistable types(classes).
    """
    def __init__(self, cls):
        self.cls = cls

    @property
    def type_id(self):
        cls = self.cls

        if issubclass(cls, PersistableMeta):
            return cls.type_id
        else:
            return cls.__name__

    @property
    def relationships(self):
        from kaiso.attributes.bases import _is_relationship_reference
        relationships = dict(getmembers(
            self.cls, _is_relationship_reference
        ))
        return relationships

    @property
    def attributes(self):
        if issubclass(self.cls, Attribute):
            attributes = {}
            for name, value in getmembers(self.cls):
                if not (name.startswith('_')
                        or isfunction(value)
                        or isdatadescriptor(value)):
                    attr = Attribute()
                    attributes[name] = attr
        else:
            attributes = dict(getmembers(self.cls, _is_attribute))

        return attributes

    @property
    def declared_attributes(self):
        declared = {}
        for name, attr in getmembers(self.cls, _is_attribute):
            if get_declaring_class(self.cls, name) == self.cls:
                declared[name] = attr

        return declared


class Persistable(object):
    """ The base of all persistable objects.

    Any object stored in the db must inherit from this class.
    Any object having Persistable as it's base are considered persistable.
    """


class DiscriptorType(type):
    def __init__(cls, name, bases, dct):  # pylint:disable-msg=W0613
        super(DiscriptorType, cls).__init__(cls)
        cls.descriptors = {}
        cls.register(cls)


class AttributeBase(object):
    name = None


class PersistableMeta(type, Persistable):
    __metaclass__ = DiscriptorType

    index_name = 'persistablemeta'
    type_id = 'PersistableMeta'

    def __new__(mcs, name, bases, dct):
        for attr_name, attr in dct.items():
            if isinstance(attr, AttributeBase):
                attr.name = attr_name

        cls = super(PersistableMeta, mcs).__new__(mcs, name, bases, dct)
        mcs.register(cls)
        return cls

    @classmethod
    def register(mcs, cls):
        name = cls.__name__
        if name in mcs.descriptors:
            raise TypeAlreadyRegistered(cls)
        mcs.descriptors[name] = Descriptor(cls)

    @classmethod
    def get_class_by_id(mcs, cls_id):
        try:
            if mcs is not PersistableMeta and issubclass(mcs, PersistableMeta):
                return PersistableMeta.get_class_by_id(cls_id)
        except UnknownType:
            pass

        return mcs.get_descriptor_by_id(cls_id).cls

    @classmethod
    def get_descriptor(mcs, cls):
        name = cls.__name__
        return mcs.get_descriptor_by_id(name)

    @classmethod
    def get_descriptor_by_id(mcs, cls_id):
        try:
            return mcs.descriptors[cls_id]
        except KeyError:
            if mcs is not PersistableMeta and issubclass(mcs, PersistableMeta):
                return PersistableMeta.get_descriptor_by_id(cls_id)

            raise UnknownType('Unknown type "{}"'.format(cls_id))

    @classmethod
    def get_index_entries(mcs, obj):
        if isinstance(obj, type):
            yield (mcs.index_name, 'id', obj.__name__)
        else:
            obj_type = type(obj)
            descr = mcs.get_descriptor(obj_type)

            for name, attr in descr.attributes.items():
                if attr.unique:
                    declaring_class = get_declaring_class(descr.cls, name)

                    index_name = get_index_name(declaring_class)
                    key = name
                    value = attr.to_db(getattr(obj, name))

                    if value is not None:
                        yield (index_name, key, value)


PersistableMeta.register(type)
PersistableMeta.register(object)


def _is_attribute(obj):
    return isinstance(obj, Attribute)


class Attribute(AttributeBase):
    __metaclass__ = PersistableMeta

    unique = None
    required = None

    def __init__(self, unique=False, required=False):
        self.unique = unique
        self.required = required

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_db(value):
        return value


class DefaultableAttribute(Attribute):
    default = None

    def __init__(self, default=None, **kwargs):
        super(DefaultableAttribute, self).__init__(**kwargs)
        self.default = default


class AttributedBase(Persistable):
    """ The base class for objects that can have Attributes.

    Sets default values during instance creation and applies kwargs
    passed to __init__.
    """
    __metaclass__ = PersistableMeta

    def __new__(cls, *args, **kwargs):

        obj = super(AttributedBase, cls).__new__(cls, *args, **kwargs)

        descriptor = type(cls).get_descriptor(cls)

        for name, attr in descriptor.attributes.items():
            setattr(obj, name, attr.default)

        return obj

    def __init__(self, *args, **kwargs):
        super(AttributedBase, self).__init__(*args, **kwargs)

        for key, value in kwargs.items():
            setattr(self, key, value)


class Entity(AttributedBase):
    def __getattribute__(self, name):
        cls = type(self)
        descriptor = type(cls).get_descriptor(cls)
        try:
            rel_reference = descriptor.relationships[name]
        except KeyError:
            return object.__getattribute__(self, name)
        else:
            return rel_reference.get_manager(self)


class Relationship(AttributedBase):
    def __init__(self, start=None, end=None, **kwargs):
        super(Relationship, self).__init__(**kwargs)

        self.start = start
        self.end = end
