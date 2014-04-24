import pytest

from kaiso.attributes import Integer
from kaiso.types import Entity, Attribute


fixture = pytest.mark.usefixtures('storage')


def test_save(manager):
    class Wrapper(object):
        def __init__(self, value=None):
            self._value = value

        def unwrap(self):
            return self._value

        def __eq__(self, other):
            return self.unwrap() == other.unwrap()

    class WrappedAttr(Attribute):
        default = Wrapper()

        @classmethod
        def to_primitive(cls, value, for_db):
            return value.unwrap()

        @classmethod
        def to_python(cls, value):
            return Wrapper(value)

    class WrappingSpam(Entity):
        id = Integer(default=1, unique=True)
        wrapped = WrappedAttr()

    manager.save(WrappingSpam)
    spam = WrappingSpam()
    manager.save(spam)

    obj = manager.get(WrappingSpam, id=spam.id)

    assert obj.wrapped == spam.wrapped
