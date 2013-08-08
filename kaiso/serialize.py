from kaiso.attributes.bases import get_attibute_for_type
from kaiso.iter_helpers import unique
from kaiso.relationships import InstanceOf, IsA


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

    for key in old.keys():
        if key not in new:
            raise KeyError('missing key: {}'.format(key))

    return changes


@unique
def get_type_relationships(obj):
    """ Generates a list of the type relationships of an object.
    e.g.
        get_type_relationships(Entity())

        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (Entity, IsA, object),
        (Entity, InstanceOf, PersistableType),
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
        for base_idx, base in enumerate(obj.__bases__):
            for item in get_type_relationships(base):
                yield item

            yield obj, (IsA, base_idx), base

    yield obj, (InstanceOf, 0), obj_type


def object_to_db_value(obj):
    try:
        attr_cls = get_attibute_for_type(type(obj))
    except KeyError:
        return obj
    else:
        return attr_cls.to_primitive(obj, for_db=True)


def dict_to_db_values_dict(data):
    return dict((k, object_to_db_value(v)) for k, v in data.items())
