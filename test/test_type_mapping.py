from orp.types import PersistableType, Persistable
from orp.persistence import object_to_dict


class FooType(PersistableType):
    pass


class Foo(object):
    __metaclass__ = FooType


def test_types_to_dict():
    dct = object_to_dict(PersistableType)
    assert dct == {'__type__': 'PersistableType', 'name': 'PersistableType'}

    dct = object_to_dict(FooType)
    assert dct == {'__type__': 'FooType', 'name': 'FooType'}


def test_classes_to_dict():
    dct = object_to_dict(Persistable)
    assert dct == {'__type__': 'PersistableType', 'name': 'Persistable'}

    dct = object_to_dict(Foo)
    assert dct == {'__type__': 'FooType', 'name': 'Foo'}


def test_objects_to_dict():
    dct = object_to_dict(Persistable())
    assert dct == {'__type__': 'Persistable'}

    dct = object_to_dict(Foo())
    assert dct == {'__type__': 'Foo'}
