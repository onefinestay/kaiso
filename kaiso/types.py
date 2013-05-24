from inspect import getmembers, getmro

from kaiso.exceptions import (UnknownType, TypeAlreadyRegistered,
                              DeserialisationError, DuplicateTypeName)


class Persistable(object):
    """ The base of all persistable objects.

    Any object stored in the db must inherit from this class.
    Any object having Persistable as it's base are considered persistable.
    """


class PersistableCollector(type, Persistable):
    """ Collects Persistable objects so that they can later be
    """
    collected = {}

    def __new__(mcs, name, bases, dct):
        cls = super(PersistableCollector, mcs).__new__(mcs, name, bases, dct)
        mcs.collect(cls)
        return cls

    @classmethod
    def collect(mcs, cls):
        name = cls.__name__
        if name in mcs.collected:
            raise DuplicateTypeName("Type `{}` already defined.".format(name))
        mcs.collected[name] = cls


class TypeRegistry():

    index_name = "persistabletype"

    def __init__(self):
        self._descriptors = {
            'static': {},
            'dynamic': {}
        }

    def initialize(self):
        for cls in PersistableCollector.collected.values():
            self.register(cls)
        self.register(PersistableCollector)

    def is_registered(self, cls):
        return self.is_dynamic_type(cls) or self.is_static_type(cls)

    def is_dynamic_type(self, cls):
        name = cls.__name__ if isinstance(cls, type) else cls
        return name in self._descriptors['dynamic']

    def is_static_type(self, cls):
        name = cls.__name__ if isinstance(cls, type) else cls
        return name in self._descriptors['static']

    def create(self, cls_id, bases, attrs):
        """ Create and register a dynamic type
        """
        cls = type(cls_id, bases, attrs)
        self.register(cls, registry="dynamic")
        return cls

    def register(self, cls, registry="static"):
        name = cls.__name__
        if name in self._descriptors[registry]:
            raise TypeAlreadyRegistered(cls)
        self._descriptors[registry][name] = Descriptor(cls)

    def get_class_by_id(self, cls_id):
        """ Return the class for a given ``cls_id``, preferring statically
        registered classes.

        Returns the statically registered class with the given ``cls_id``, iff
        one exists, and otherwise returns any dynamically registered class
        with that id.

        Arguments:
            cls_id: id of the class to return
            registry: type of registered class to prefer

        Returns:
            The class that was registered with ``cls_id``
        """
        try:
            return self._descriptors['static'][cls_id].cls
        except KeyError:
            return self.get_descriptor_by_id(cls_id).cls

    def get_descriptor(self, cls):
        name = cls.__name__
        return self.get_descriptor_by_id(name)

    def get_descriptor_by_id(self, cls_id):
        """ Return the Descriptor for a given ``cls_id``.

        Returns the descriptor for the class registered with the given
        ``cls_id``. If dynamic types have not been loaded yet, return the
        descriptor of the statically registered class.

        Arguments:
            cls_id: id of the class

        Returns:
            The Descriptor for that cls_id

        Raises:
            UnknownType if no type has been registered with the given id.
        """
        if not self.is_registered(cls_id):
            raise UnknownType('Unknown type "{}"'.format(cls_id))

        try:
            return self._descriptors['dynamic'][cls_id]
        except KeyError:
            return self._descriptors['static'][cls_id]

    def get_index_entries(self, obj):
        if isinstance(obj, type):
            yield (self.index_name, 'id', obj.__name__)
        else:
            obj_type = type(obj)
            descr = self.get_descriptor(obj_type)

            for name, attr in descr.attributes.items():
                if attr.unique:
                    declaring_class = get_declaring_class(descr.cls, name)

                    index_name = get_index_name(declaring_class)
                    key = name
                    value = attr.to_db(getattr(obj, name))

                    if value is not None:
                        yield (index_name, key, value)

    def object_to_dict(self, obj, include_none=True):
        """ Converts a persistable object to a dict.

        The generated dict will contain a __type__ key, for which the value
        will be the type_id as given by the descriptor for type(obj).

        If the object is a class, dict will contain an id-key with the value
        being the type_id given by the descriptor for the object.

        For any other object all the attributes as given by the object's
        type descriptpr will be added to the dict and encoded as required.

        Args:
            obj: A persistable  object.

        Returns:
            Dictionary with attributes encoded in basic types
            and type information for deserialization.
            e.g.
            {
                '__type__': 'Entity',
                'attr1' : 1234
            }
        """
        obj_type = type(obj)

        descr = self.get_descriptor(obj_type)

        properties = {
            '__type__': descr.type_id,
        }

        if isinstance(obj, type):
            properties['id'] = self.get_descriptor(obj).type_id

        else:
            for name, attr in descr.attributes.items():
                try:
                    value = attr.to_db(getattr(obj, name))
                except AttributeError:
                    # if we are dealing with an extended type, we may not
                    # have the attribute set on the instance
                    if isinstance(attr, DefaultableAttribute):
                        value = attr.default
                    else:
                        value = None

                if value is not None or include_none:
                    properties[name] = value

        return properties

    def dict_to_object(self, properties):
        """ Converts a dict into a persistable object.

        The properties dict needs at least a __type__ key containing the name
        of any registered class.
        The type key defines the type of the object to return.

        If the registered class for the __type__ is a meta-class,
        i.e. a subclass of <type>, a name key is assumed to be present and
        the registered class idendified by it's value is returned.

        If the registered class for the __type__ is standard class,
        i.e. an instance of <type>, and object of that class will be created
        with attributes as defined by the remaining key-value pairs.

        Args:
            properties: A dict like object.

        Returns:
            A persistable object.
        """

        try:
            type_id = properties['__type__']
        except KeyError:
            raise DeserialisationError(
                'properties "{}" missing __type__ key'.format(properties))

        # if type_id == Descriptor(PersistableMeta).type_id:
        if type_id == self.index_name:
            # we are looking at a class object
            cls_id = properties['id']
        else:
            # we are looking at an instance object
            cls_id = type_id

        cls = self.get_class_by_id(cls_id)

        if cls_id != type_id:
            return cls
        else:
            obj = cls.__new__(cls)

            descr = self.get_descriptor_by_id(cls_id)

            for attr_name, attr in descr.attributes.items():
                try:
                    value = properties[attr_name]
                except KeyError:
                    pass
                else:
                    value = attr.to_python(value)
                    setattr(obj, attr_name, value)

        return obj


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
        attributes = dict(getmembers(self.cls, _is_attribute))

        if issubclass(self.cls, Attribute):
            # Because we don't have the attrs on the base Attribute classes
            # declared using Attribute instances, we have to pretend we did,
            # so that they behave like them.
            attributes['name'] = Attribute()
            attributes['unique'] = Attribute()
            attributes['required'] = Attribute()

            if issubclass(self.cls, DefaultableAttribute):
                attributes['default'] = Attribute()

        return attributes

    @property
    def declared_attributes(self):
        declared = {}
        for name, attr in getmembers(self.cls, _is_attribute):
            if get_declaring_class(self.cls, name) == self.cls:
                declared[name] = attr

        return declared


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
    __metaclass__ = PersistableCollector

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
    __metaclass__ = PersistableCollector

    def __new__(cls, *args, **kwargs):

        obj = super(AttributedBase, cls).__new__(cls, *args, **kwargs)

        descriptor = Descriptor(cls)

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
        descriptor = Descriptor(cls)
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
