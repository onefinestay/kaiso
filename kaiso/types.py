from __future__ import absolute_import  # local types.py and builtin types

from contextlib import contextmanager
from functools import wraps
from inspect import getmembers, getmro

from kaiso.exceptions import (
    UnknownType, TypeAlreadyRegistered, TypeAlreadyCollected,
    DeserialisationError)


# at some point, rename id to __name__ and just skip all dunder attrs
INTERNAL_CLASS_ATTRS = ['__type__', 'id']
CLASS_ATTRIBUTE_TYPES = (basestring, int, bool, list, float)


class Persistable(object):
    """ The base of all persistable objects.

    Any object stored in the db must inherit from this class.
    Any object having Persistable as it's base are considered persistable.
    """


class StaticClassCollector(object):
    def __init__(self):
        self.reset_state()

    def add_class(self, cls):
        name = get_type_id(cls)
        if name in self.classes:
            raise TypeAlreadyCollected(
                "Type `{}` already defined.".format(name)
            )
        self.classes[name] = cls
        self.descriptors[name] = Descriptor(cls)

    def dump_state(self):
        return self.classes.copy(), self.descriptors.copy()

    def load_state(self, state):
        self.classes, self.descriptors = state
        # make sure we reset all descriptor caches
        for descriptor in self.descriptors.values():
            descriptor._clear_cache()

    def reset_state(self):
        self.load_state(({}, {}))

    def get_descriptors(self):
        return self.descriptors

    def get_classes(self):
        return self.classes


collected_static_classes = StaticClassCollector()


@contextmanager
def collector(type_map=None):
    """ Allows code-defined types to be collected into a custom dict.

    Example:

    with collector() as type_map:
        class Foobar(Entity):
            pass

    type_map == {'Foobar': Foobar}
    """
    if type_map is None:
        type_map = {}

    state = collected_static_classes.dump_state()
    try:

        collected_static_classes.reset_state()
        yield collected_static_classes.get_classes()
    finally:
        collected_static_classes.load_state(state)


class PersistableType(type, Persistable):
    """ Metaclass for static persistable types.

    Collects classes as they are declared so that they can be registered with
    the TypeRegistry later.

    Collection can be controlled using the ``collector`` context manager.
    """
    def __new__(mcs, name, bases, dct):
        if "__type__" in dct:
            raise TypeError("__type__ is a reserved attribute")

        cls = super(PersistableType, mcs).__new__(mcs, name, bases, dct)
        collected_static_classes.add_class(cls)
        return cls


class TypeRegistry(object):
    """ Keeps track of statically and dynamically declared types.
    """
    _builtin_types = (PersistableType,)

    def __init__(self):
        self._dynamic_descriptors = {}
        self._types_in_db = set()

    @property
    def _static_descriptors(self):
        return collected_static_classes.get_descriptors()

    def is_static_type(self, cls):
        class_id = get_type_id(cls)
        descriptor = self._static_descriptors.get(class_id, False)
        return descriptor and descriptor.cls == cls

    def has_code_defined_attribute(self, cls, attr_name):
        """ Determine whether an attribute called ``attr_name`` was defined
        in code on ``cls`` or one of its parent types.
        """
        # find the declaring class in the code-defined hierarchy
        class_id = get_type_id(cls)
        maybe_static = self.get_class_by_id(class_id)
        declaring_cls = get_declaring_class(maybe_static, attr_name,
                                            prefer_subclass=False)
        if not declaring_cls:
            return False  # attribute not found

        # given a declaring class, determine whether that class is dynamic or
        # code-defined
        cls = declaring_cls
        class_id = get_type_id(cls)
        # get_descriptor_by_id will return a dynamically defined class with
        # the given class_id, if there is one. otherwise it falls back to
        # a code-defined class
        maybe_dynamic = self.get_descriptor_by_id(class_id).cls
        # get_class_by_id will return a code-defined class with the given
        # class_id, if there is one. otherwise it falls back to one that
        # has been rehydrated from graph data
        maybe_static = self.get_class_by_id(class_id)

        if maybe_dynamic is maybe_static:
            # type is purely dynamic or purely static
            if not self.is_static_type(maybe_static):
                return False

        is_static_attr = hasattr(maybe_static, attr_name)
        return is_static_attr

    def create_type(self, cls_id, bases, attrs):
        """ Create and register a dynamic type
        """

        with collector():
            cls = PersistableType(cls_id, bases, attrs)

        self.register(cls)

        return cls

    def register(self, cls):
        """ Register a dynamic type
        """
        descriptors = self._dynamic_descriptors

        name = get_type_id(cls)
        if name in descriptors:
            raise TypeAlreadyRegistered(cls)

        descriptors[name] = Descriptor(cls)

    def get_class_by_id(self, cls_id):
        """ Return the class for a given ``cls_id``, preferring statically
        registered classes.

        Returns the statically registered class with the given ``cls_id``, if
        one exists, and otherwise returns any dynamically registered class
        with that id.

        Arguments:
            cls_id: id of the class to return
            registry: type of registered class to prefer

        Returns:
            The class that was registered with ``cls_id``
        """
        try:
            return self._static_descriptors[cls_id].cls
        except KeyError:
            return self.get_descriptor_by_id(cls_id).cls

    def refresh_type(self, cls):
        descriptor = self.get_descriptor(cls)
        descriptor._clear_cache()

    def get_descriptor(self, cls):
        name = get_type_id(cls)
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
        if cls_id in self._dynamic_descriptors:
            return self._dynamic_descriptors[cls_id]
        elif cls_id in self._static_descriptors:
            return self._static_descriptors[cls_id]
        else:
            raise UnknownType('Unknown type "{}"'.format(cls_id))

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
            descr = self.get_descriptor(obj)
            if for_db:
                class_attributes = descr.declared_class_attributes
            else:
                class_attributes = descr.class_attributes

            for name, attr in class_attributes.items():
                # note that we only support native types as class attributes
                properties[name] = getattr(obj, name)

        else:
            descr = self.get_descriptor(obj_type)
            for name, attr in descr.attributes.items():
                try:
                    obj_value = getattr(obj, name)
                    value = attr.to_primitive(obj_value, for_db=for_db)
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
            cls = self.get_class_by_id(cls_id)
            descr = self.get_descriptor_by_id(cls_id)

            for attr_name, value in properties.items():
                if attr_name in INTERNAL_CLASS_ATTRS:
                    continue
                # these are already native (we only support native class attrs)
                setattr(cls, attr_name, value)
            return cls

        # we are looking at an instance object
        cls_id = type_id
        cls = self.get_class_by_id(cls_id)
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
        clone._dynamic_descriptors = self._dynamic_descriptors.copy()
        clone._types_in_db = self._types_in_db.copy()
        return clone


def get_declaring_class(cls, attr_name, prefer_subclass=True):
    """ Returns the class in the type heirarchy of ``cls`` that defined
        an attribute with name ``attr_name``.

        If ``prefer_subclass`` is false, return the last class in the MRO
        that defined an attribute with the given name.
    """
    declared_attr = None
    declaring_class = None

    # Start at the top of the hierarchy and determine which of the MRO have
    # the attribute. Return the lowest class that defines (or overloads) the
    # attribute, unless prefer_subclass is False.
    for base in reversed(getmro(cls)):
        sentinel = object()
        attr = getattr(base, attr_name, sentinel)
        if attr is not sentinel and declared_attr is not attr:
            declaring_class = base
            declared_attr = attr
            if not prefer_subclass:
                break

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


def cache_result(func):
    """Cache the result of a function in self._cache
    """
    @wraps(func)
    def decorated(self):
        cache_key = '_{}'.format(func.__name__)

        if cache_key not in self._cache:
            result = func(self)
            self._cache[cache_key] = result
        return self._cache[cache_key]

    return decorated


class Descriptor(object):
    """ Provides information about the types of persistable objects.

    Its main purpose is to provide type names and attribute information of
    persistable types(classes).
    """
    def __init__(self, cls):
        self.cls = cls
        self._cache = {}

    def _clear_cache(self):
        self._cache = {}

    @property
    @cache_result
    def relationships(self):
        from kaiso.attributes.bases import _is_relationship_reference
        relationships = dict(getmembers(
            self.cls, _is_relationship_reference
        ))
        return relationships

    @property
    @cache_result
    def attributes(self):
        attributes = dict(getmembers(self.cls, _is_attribute))

        if issubclass(self.cls, AttributeBase):
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
    @cache_result
    def declared_attributes(self):
        declared = {}
        for name, attr in getmembers(self.cls, _is_attribute):
            if get_declaring_class(self.cls, name) == self.cls:
                declared[name] = attr

        return declared

    @property
    @cache_result
    def class_attributes(self):
        attributes = {}
        for name, value in getmembers(self.cls, _is_class_attribute):
            if name.startswith('__'):
                continue
            attributes[name] = value
        return attributes

    @property
    @cache_result
    def declared_class_attributes(self):
        declared = {}
        for name, value in self.class_attributes.items():
            if get_declaring_class(self.cls, name) == self.cls:
                declared[name] = value

        return declared


class AttributeBase(object):
    __metaclass__ = PersistableType

    name = None

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_primitive(value, for_db):
        """ Serialize ``value`` to a primitive type suitable for inserting
            into the database or passing to e.g. ``json.dumps``
        """
        return value


def _is_attribute(obj):
    return isinstance(obj, AttributeBase)


def _is_class_attribute(obj):
    return isinstance(obj, CLASS_ATTRIBUTE_TYPES)


class Attribute(AttributeBase):
    unique = None
    required = None

    def __init__(self, unique=False, required=False):
        self.unique = unique
        self.required = required


class DefaultableAttribute(Attribute):
    default = None

    def __init__(self, default=None, **kwargs):
        super(DefaultableAttribute, self).__init__(**kwargs)
        self.default = default

    def __eq__(self, other):
        same_type = type(self) is type(other)
        equal_default = self.default == other.default
        return same_type and equal_default


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
