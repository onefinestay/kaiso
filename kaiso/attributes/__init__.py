import decimal
import uuid
import datetime
import iso8601

from kaiso.attributes.bases import RelationshipReference, wraps_type
from kaiso.types import Attribute, DefaultableAttribute


class Outgoing(RelationshipReference):
    pass


class Incoming(RelationshipReference):
    pass


@wraps_type(uuid.UUID)
class Uuid(Attribute):
    @property
    def default(self):
        return uuid.uuid4()

    @staticmethod
    def to_primitive(value, for_db):
        if value is None:
            return None
        return str(value)

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return uuid.UUID(hex=value)


class Bool(DefaultableAttribute):
    pass


class Integer(DefaultableAttribute):
    pass


class Float(DefaultableAttribute):
    pass


class String(DefaultableAttribute):
    pass


class Tuple(DefaultableAttribute):
    @staticmethod
    def to_primitive(value, for_db):
        if value is None:
            return None
        return list(value)

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return tuple(value)


@wraps_type(decimal.Decimal)
class Decimal(DefaultableAttribute):
    @staticmethod
    def to_primitive(value, for_db):
        if value is None:
            return None
        return str(value)

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return decimal.Decimal(value)


@wraps_type(datetime.datetime)
class DateTime(DefaultableAttribute):
    @staticmethod
    def to_primitive(value, for_db):
        if value is None:
            return None
        return value.isoformat()

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return iso8601.parse_date(value)


class Choice(DefaultableAttribute):
    choices = Tuple(default=tuple(), required=True)

    def __init__(self, *choices, **kwargs):
        super(Choice, self).__init__(**kwargs)
        self.choices = choices
