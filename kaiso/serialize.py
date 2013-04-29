from kaiso.attributes.bases import get_attibute_for_type
from kaiso.exceptions import DeserialisationError
from kaiso.iter_helpers import unique
from kaiso.relationships import InstanceOf, IsA
from kaiso.types import (
    Attribute, DefaultableAttribute, Descriptor, PersistableMeta)


def get_changes(old, new):
    """Return a changes dictionary containing the key/values in new that are
       different from old. Any key in old that is not in new will have a None
       value in the resulting dictionary
    """
    changes = {}

    # check for any keys that have changed, put their new value in
    for key, value in new.items():
        if old.get(key) != value:
            changes[key] = value

    # if a key has dissappeared in new, put a None in changes, which
    # will remove it in neo
    for key in old.keys():
        if key not in new:
            raise KeyError('missing key: {}'.format(key))
            # changes[key] = None

    return changes


@unique
def get_type_relationships(obj):
    """ Generates a list of the type relationships of an object.
    e.g.
        get_type_relationships(Entity())

        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (PersistableMeta, InstanceOf, type),
        (Entity, IsA, object),
        (Entity, InstanceOf, PersistableMeta),
        (<Entity object>, InstanceOf, Entity)

    Args:
        obj:    An object to generate the type relationships for.

    Returns:
        A generator, generating tuples
            (object, relatsionship type, related obj)
    """
    obj_type = type(obj)

    if obj_type is not type:
        for item in get_type_relationships(obj_type):
            yield item

    if isinstance(obj, type):
        for base in obj.__bases__:
            for item in get_type_relationships(base):
                yield item
            yield obj, IsA, base

    yield obj, InstanceOf, obj_type


def object_to_db_value(obj):
    try:
        attr_cls = get_attibute_for_type(type(obj))
    except KeyError:
        return obj
    else:
        return attr_cls.to_db(obj)


def dict_to_db_values_dict(data):
    return dict((k, object_to_db_value(v)) for k, v in data.items())


def object_to_dict(obj, dynamic_type=PersistableMeta, include_none=True):
    """ Converts a persistable object to a dict.

    The generated dict will contain a __type__ key, for which the value
    will be the type_id as given by the descriptor for type(obj).

    If the object is a class a name key-value pair will be
    added to the generated dict, with the value being the type_id given
    by the descriptor for the object.

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
        '__type__': Descriptor(obj_type).type_id,
    }

    if isinstance(obj, type):
        properties['id'] = Descriptor(obj).type_id

    elif isinstance(obj, Attribute):
        # TODO: move logic to handle Attribute attrs into descriptor
        #       and let this code just treat them like any other persistable
        properties['unique'] = obj.unique
        properties['required'] = obj.required

        if isinstance(obj, DefaultableAttribute):
            if obj.default is not None or include_none:
                properties['default'] = obj.default

    else:
        descr = dynamic_type.get_descriptor(obj_type)

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


def dict_to_object(properties, dynamic_type=PersistableMeta):
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

    if type_id == Descriptor(PersistableMeta).type_id:
        # we are looking at a class object
        cls_id = properties['id']
    else:
        # we are looking at an instance object
        cls_id = type_id

    cls = dynamic_type.get_class_by_id(cls_id)

    if cls_id != type_id:
        return cls
    else:
        obj = cls.__new__(cls)

        if isinstance(obj, Attribute):
            for attr_name, value in properties.iteritems():
                setattr(obj, attr_name, value)
        else:
            descr = dynamic_type.get_descriptor_by_id(cls_id)

            for attr_name, attr in descr.attributes.items():
                try:
                    value = properties[attr_name]
                except KeyError:
                    pass
                else:
                    value = attr.to_python(value)
                    setattr(obj, attr_name, value)

    return obj
