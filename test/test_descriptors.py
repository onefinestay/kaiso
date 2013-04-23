from kaiso.descriptors import get_descriptor
from kaiso.types import PersistableMeta, Entity


class Foo(Entity):
    pass


def test_type_names():
    descr = get_descriptor(PersistableMeta)
    assert descr.type_name == 'PersistableMeta'

    descr = get_descriptor(Entity)
    assert descr.type_name == 'Entity'

    descr = get_descriptor(Foo)
    assert descr.type_name == 'Foo'


def test_index_names():
    descr = get_descriptor(PersistableMeta)
    assert descr.get_index_name_for_attribute() == 'persistablemeta'
