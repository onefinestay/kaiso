import decimal
import uuid

import iso8601

from orp.attributes.bases import (
    Attribute, DefaultableAttribute, RelationshipReference, )


many = '*'


class Outgoing(RelationshipReference):
    pass


class Incoming(RelationshipReference):
    pass


class Uuid(Attribute):
    @property
    def default(self):
        return uuid.uuid4()

    @staticmethod
    def to_db(value):
        return str(value)

    @staticmethod
    def to_python(value):
        return uuid.UUID(hex=value)


class Bool(DefaultableAttribute):
    pass


class Integer(DefaultableAttribute):
    pass


class Float(DefaultableAttribute):
    pass


class String(DefaultableAttribute):
    pass


class Decimal(DefaultableAttribute):
    @staticmethod
    def to_db(value):
        if value is None:
            return None
        return str(value)

    @staticmethod
    def to_python(value):
        return decimal.Decimal(value)


class DateTime(DefaultableAttribute):
    @staticmethod
    def to_db(value):
        if value is None:
            return None
        return value.isoformat()

    @staticmethod
    def to_python(value):
        return iso8601.parse_date(value)


class Choice(String):
    def __init__(self, *choices, **kwargs):
        # again, super(Choice) vs Choice.__init__
        super(Choice, self).__init__(**kwargs)
        self.choices = choices
