import decimal
import uuid

import iso8601


class Attribute(object):
    def __init__(self, unique=False):
        self.unique = unique

    @staticmethod
    def to_python(value):
        return value

    @staticmethod
    def to_db(value):
        return value


class DefaultableAttribute(Attribute):
    def __init__(self, default=None, unique=False):
        # do we have a preference for using super() over <BaseClass>.<method> ?
        super(DefaultableAttribute, self).__init__(unique)
        self.default = default


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

