import pytest

from kaiso.attributes import Integer
from kaiso.types import Entity, Attribute


fixture = pytest.mark.usefixtures('storage')


class Wrapper(object):
    def __init__(self, value=None):
        self._value = value

    def unwrap(self):
        return self._value

    def __eq__(self, other):
        return self.unwrap() == other.unwrap()


class WrappedAttr(Attribute):
    default = Wrapper()

    @staticmethod
    def to_db(value):
        return value.unwrap()

    @staticmethod
    def to_python(value):
        return Wrapper(value)


class WrappingSpam(Entity):
    id = Integer(default=1)
    wrapped = WrappedAttr()


def test_save(storage):
    spam = WrappingSpam()
    storage.save(spam)

    obj = storage.get(WrappingSpam, id=spam.id)

    assert obj.wrapped == spam.wrapped
