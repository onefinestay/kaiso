from orp.descriptors import register_type, get_descriptor, is_attribute

register_type(type)
register_type(object)

@register_type
class PersistableType(type):
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


class Persistable(object):
    __metaclass__ = PersistableType

    def __new__(cls, *args, **kwargs):
        # setup default values for attributes
        obj = super(Persistable, cls).__new__(cls)
        descriptor = get_descriptor(cls)
        for name, attr in descriptor.members.items():
            setattr(obj, name, attr.default)
        return obj

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)



