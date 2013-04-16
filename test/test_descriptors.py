from orp.descriptors import get_descriptor
from orp.types import PersistableType, Persistable


class FooType(PersistableType):
    pass


class Foo(object):
    __metaclass__ = FooType


def test_type_names():
    descr = get_descriptor(PersistableType)
    assert descr.type_name == 'PersistableType'

    descr = get_descriptor(FooType)
    assert descr.type_name == 'FooType'

    descr = get_descriptor(Persistable)
    assert descr.type_name == 'Persistable'

    descr = get_descriptor(Foo)
    assert descr.type_name == 'Foo'


def test_index_names():
    descr = get_descriptor(PersistableType)
    assert descr.get_index_name_for_attribute() == 'persistabletype'

    descr = get_descriptor(FooType)
    assert descr.get_index_name_for_attribute() == 'footype'
