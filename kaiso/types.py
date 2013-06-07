from inspect import getmembers, getmro
from kaiso.exceptions import (
    UnknownType, TypeAlreadyRegistered, TypeAlreadyCollected,
    DeserialisationError)


class Persistable(object):
    """ The base of all persistable objects.

    Any object stored in the db must inherit from this class.
    Any object having Persistable as it's base are considered persistable.
    """

collected_static_classes = {}


def collect_class(cls):
    """ Collect a class as it is defined at import time. Called by the
    PersistableType metaclass at class creation time.
    """
    name = cls.__name__
    if name in collected_static_classes:
        raise TypeAlreadyCollected(
            "Type `{}` already defined.".format(name)
        )
    collected_static_classes[name] = cls


class PersistableType(type, Persistable):
    """ Metaclass for static persistable types.

    Collects classes as they are declared so that they can be registered with
    the TypeRegistry later.
    """
    def __new__(mcs, name, bases, dct):
        cls = super(PersistableType, mcs).__new__(mcs, name, bases, dct)
        # DynamicType must inherit from PersistableType, but we only want to
        # collect statically defined classes
        if mcs is not DynamicType:
            collect_class(cls)
        return cls


class DynamicType(PersistableType):
    """ Metaclass for dynamic persistable types

    Extends PersistableType, but the classes it produces will not be
    collected.
    """


class TypeRegistry(object):
    """ Keeps track of statically and dynamically declared types.
    """
    _static_descriptors = {}
    _builtin_types = (PersistableType, DynamicType)

    def __init__(self):
        self._descriptors = {
            'static': self._static_descriptors,
            'dynamic': {}
        }

        if not self._static_descriptors:
            for type_ in self._builtin_types:
                self.register(type_)
        self._sync_static_descriptors()

    @classmethod
    def _sync_static_descriptors(cls):
        # exit early if there've been no changes to the statically
        # collected types
        num_registered = len(cls._static_descriptors)
        static_registered = num_registered - len(cls._builtin_types)
        if static_registered == len(collected_static_classes):
            return

        for name, collected_cls in collected_static_classes.iteritems():
            if name not in cls._static_descriptors:
                cls._static_descriptors[name] = Descriptor(collected_cls)

    def is_registered(self, cls):
        """ Determine if ``cls`` is a registered type.

        ``cls`` may be the name of the class, or the class object itself.

        Arguments:
            - cls: The class object or a class name as a string

        Returns:
            True if ``cls`` is registered as a static or dynamic type, False
            otherwise.
        """
        name = cls.__name__ if isinstance(cls, type) else cls
        return (name in self._descriptors['static'] or
                name in self._descriptors['dynamic'])

    def is_dynamic_type(self, cls):
        """ Determine if ``cls`` is a dynamic type.

        ``cls`` does not need to be registered with this type registry.

        Arguments:
            - cls: The class object to inspect

        Returns:
            True if ``cls`` is a dynamic type, False otherwise
        """
        return type(cls) is DynamicType

    def is_static_type(self, cls):
        """ Determine if ``cls`` is a static type.

        ``cls`` does not need to be registered with this type registry.

        Arguments:
            - cls: The class object to inspect

        Returns:
            True if ``cls`` is a static type, False otherwise
        """
        return type(cls) is PersistableType

    def create_type(self, cls_id, bases, attrs):
        """ Create and register a dynamic type
        """
        cls = DynamicType(cls_id, bases, attrs)
        self.register(cls)
        return cls

    def register(self, cls):
        """ Register a type
        """
        name = cls.__name__
        registry = "dynamic" if type(cls) is DynamicType else "static"

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
        self._sync_static_descriptors()
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
        self._sync_static_descriptors()
        if not self.is_registered(cls_id):
            raise UnknownType('Unknown type "{}"'.format(cls_id))

        try:
            return self._descriptors['dynamic'][cls_id]
        except KeyError:
            return self._descriptors['static'][cls_id]

    def get_index_entries(self, obj):
        if isinstance(obj, PersistableType):
            yield (get_index_name(PersistableType), 'id', obj.__name__)
        else:
            obj_type = type(obj)
            descr = self.get_descriptor(obj_type)

            for name, attr in descr.attributes.items():
                if attr.unique:
                    declaring_class = get_declaring_class(descr.cls, name)

                    index_name = get_index_name(declaring_class)
                    key = name
                    value = attr.to_primitive(getattr(obj, name), for_db=True)

                    if value is not None:
                        yield (index_name, key, value)

    def object_to_dict(self, obj, for_db=False):
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

        properties = {
            '__type__': get_type_id(obj_type)
        }

        if isinstance(obj, type):
            properties['id'] = get_type_id(obj)

        else:
            descr = self.get_descriptor(obj_type)
            for name, attr in descr.attributes.items():
                try:
                    value = attr.to_primitive(
                        getattr(obj, name), for_db=for_db)
                except AttributeError:
                    # if we are dealing with an extended type, we may not
                    # have the attribute set on the instance
                    if isinstance(attr, DefaultableAttribute):
                        value = attr.default
                    else:
                        value = None

                if for_db and value is None:
                    continue

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

        if type_id == get_type_id(PersistableType):
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
                value = properties.get(attr_name)
                value = attr.to_python(value)
                setattr(obj, attr_name, value)

        return obj

    def clone(self):
        """Return a copy of this TypeRegistry that maintains an independent
        dynamic type registry"""
        clone = TypeRegistry()
        clone._descriptors['dynamic'] = self._descriptors['dynamic'].copy()
        return clone


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
    if issubclass(cls, PersistableType):
        return PersistableType.__name__.lower()
    else:
        return cls.__name__.lower()


def get_type_id(cls):
    """ Returns the type_id for a class.
    """
    if issubclass(cls, PersistableType):
        return PersistableType.__name__
    else:
        return cls.__name__


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


class AttributeBase(object):
    name = None


def _is_attribute(obj):
    return isinstance(obj, Attribute)


class Attribute(AttributeBase):
    __metaclass__ = PersistableType

    unique = None
    required = None

    def __init__(self, unique=False, required=False):
        self.unique = unique
        self.required = required

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_primitive(value, for_db):
        """ Serialize ``value`` to a primitive type suitable for inserting
            into the database or passing to e.g. ``json.dumps``
        """
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
    __metaclass__ = PersistableType

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
