from orp.descriptors import register_type, get_descriptor

register_type(type)
register_type(object)


@register_type
class PersistableType(type):
    def __new__(mcs, name, bases, dct):
        # This is only required for subclasses of PersistableType
        register_type(mcs)
        cls = super(PersistableType, mcs).__new__(mcs, name, bases, dct)
        register_type(cls)
        return cls

    def __init__(cls, name, bases, dct):
        super(PersistableType, cls).__init__(name, bases, dct)

        descriptor = get_descriptor(cls)
        for name, attr in dct.items():
            if name in descriptor.members:
                # TODO: assert not hasattr(attr._declared_on) ?
                attr.declared_on = cls


class AttributedBase(object):
    # MJB: Docstring please
    __metaclass__ = PersistableType

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


class Persistable(AttributedBase):
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
