from orp.descriptors import register_type, get_descriptor, is_attribute

register_type(type)
register_type(object)


@register_type
class PersistableType(type):
    # MJB: Docstring please
    def __new__(meta, name, bases, dct):
        register_type(meta)
        cls = super(PersistableType, meta).__new__(meta, name, bases, dct)
        register_type(cls)
        return cls

    def __init__(cls, name, bases, dct):
        for name, attr in dct.items():
            if is_attribute(attr):
                # TODO: assert not hasattr(attr._declared_on) ?
                attr.declared_on = cls


def get_rel_attr(attr_type):

    def p(self):
        return 'spam'

    return property(p)


class Persistable(object):
    # MJB: Docstring please
    __metaclass__ = PersistableType

    def __new__(cls, *args, **kwargs):
        # setup default values for attributes
        obj = super(Persistable, cls).__new__(cls)
        descriptor = get_descriptor(cls)

        # TODO: rename members->attributes?
        for name, attr in descriptor.members.items():
            setattr(obj, name, attr.default)

        for name, attr in descriptor.relationships.items():
            setattr(obj, name, get_rel_attr(attr))
        return obj

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)



