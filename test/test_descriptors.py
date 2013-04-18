from kaiso.descriptors import get_descriptor
from kaiso.types import PersistableType, Persistable


class Foo(Persistable):
    pass


def test_type_names():
    descr = get_descriptor(PersistableType)
    assert descr.type_name == 'PersistableType'

    descr = get_descriptor(Persistable)
    assert descr.type_name == 'Persistable'

    descr = get_descriptor(Foo)
    assert descr.type_name == 'Foo'


def test_index_names():
    descr = get_descriptor(PersistableType)
    assert descr.get_index_name_for_attribute() == 'persistabletype'
