from datetime import datetime
import decimal
import pytest
from uuid import uuid4

import iso8601

from kaiso.attributes import Uuid, Integer, Decimal, DateTime
from kaiso.types import Entity


class TestDefaultToPrimitiveDefaultToPython(object):
    @pytest.fixture
    def cls(self):
        class Foo(Entity):
            bar = Integer()
        return Foo

    def test_correct_type(self, type_registry, cls):
        instance = cls(bar=1)
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == 1
        obj = type_registry.dict_to_object(data)
        assert obj.bar == 1

    def test_coercable_type_valid_value(self, type_registry, cls):
        instance = cls(bar='1')
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == 1
        obj = type_registry.dict_to_object(data)
        assert obj.bar == 1

    def test_coercable_type_invalid_value(self, type_registry, cls):
        instance = cls(bar='invalid')
        with pytest.raises(ValueError):
            type_registry.object_to_dict(instance, for_db=True)


class TestDefaultToPrimitiveCustomToPython(object):
    @pytest.fixture
    def cls(self):
        class Foo(Entity):
            bar = Decimal()
        return Foo

    def test_correct_type(self, type_registry, cls):
        instance = cls(bar=decimal.Decimal('2.1'))
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == '2.1'
        obj = type_registry.dict_to_object(data)
        assert obj.bar == decimal.Decimal('2.1')

    def test_coercable_type_valid_value(self, type_registry, cls):
        instance = cls(bar='2.1')
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == '2.1'
        obj = type_registry.dict_to_object(data)
        assert obj.bar == decimal.Decimal('2.1')

    def test_coercable_type_invalid_value(self, type_registry, cls):
        instance = cls(bar='invalid')
        with pytest.raises(ValueError) as ex:
            type_registry.object_to_dict(instance, for_db=True)
        assert "is not a valid value for" in str(ex)


class TestCustomToPrimitiveDefaultToPython(object):
    @pytest.fixture
    def cls(self):
        class Foo(Entity):
            bar = Uuid()
        return Foo

    def test_correct_type(self, type_registry, cls):
        uuid = uuid4()
        instance = cls(bar=uuid)
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == str(uuid)
        obj = type_registry.dict_to_object(data)
        assert obj.bar == uuid

    def test_coercable_type_valid_value(self, type_registry, cls):
        uuid = uuid4()
        instance = cls(bar=str(uuid))
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == str(uuid)
        obj = type_registry.dict_to_object(data)
        assert obj.bar == uuid

    def test_coercable_type_invalid_value(self, type_registry, cls):
        instance = cls(bar='invalid')
        with pytest.raises(ValueError):
            type_registry.object_to_dict(instance, for_db=True)


class TestCustomToPrimitiveCustomToPython(object):
    @pytest.fixture
    def cls(self):
        class Foo(Entity):
            bar = DateTime()
        return Foo

    def test_correct_type(self, type_registry, cls):
        value = datetime(2012, 1, 1, 2, 3, tzinfo=iso8601.iso8601.Utc())
        str_value = '2012-01-01T02:03:00+00:00'

        instance = cls(bar=value)
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == str_value
        obj = type_registry.dict_to_object(data)
        assert obj.bar == value

    def test_coercable_type_valid_value(self, type_registry, cls):
        value = datetime(2012, 1, 1, 2, 3, tzinfo=iso8601.iso8601.Utc())
        str_value = '2012-01-01T02:03:00+00:00'

        instance = cls(bar=str_value)
        data = type_registry.object_to_dict(instance, for_db=True)
        assert data['bar'] == str_value
        obj = type_registry.dict_to_object(data)
        assert obj.bar == value

    def test_coercable_type_invalid_value(self, type_registry, cls):
        instance = cls(bar='invalid')
        with pytest.raises(ValueError):
            type_registry.object_to_dict(instance, for_db=True)

    def test_coercable_type_invalid_value_type(self, type_registry, cls):
        instance = cls(bar=object())
        with pytest.raises(ValueError):
            type_registry.object_to_dict(instance, for_db=True)
