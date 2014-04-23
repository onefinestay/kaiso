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


def with_to_primitive(primitive_type):
    """ Add a basic `to_primitive` method, coercing value to `primitive_type`
    unless it is `None`.
    """
    def decorator(cls):
        @staticmethod
        def to_primitive(value, for_db):
            if value is None:
                return None
            return primitive_type(value)
        cls.to_primitive = to_primitive
        return cls
    return decorator


@wraps_type(uuid.UUID)
@with_to_primitive(str)
class Uuid(Attribute):
    @property
    def default(self):
        return uuid.uuid4()

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return uuid.UUID(hex=value)


@with_to_primitive(bool)
class Bool(DefaultableAttribute):
    pass


@with_to_primitive(int)
class Integer(DefaultableAttribute):
    pass


@with_to_primitive(float)
class Float(DefaultableAttribute):
    pass


@with_to_primitive(unicode)
class String(DefaultableAttribute):
    pass


@wraps_type(tuple)
@with_to_primitive(list)
class Tuple(DefaultableAttribute):
    @staticmethod
    def to_python(value):
        if value is None:
            return None
        return tuple(value)


@wraps_type(decimal.Decimal)
@with_to_primitive(str)
class Decimal(DefaultableAttribute):
    @staticmethod
    def to_python(value):
        if value is None:
            return None
        try:
            return decimal.Decimal(value)
        except decimal.DecimalException as ex:
            # DecimalException doesn't inherit from ValueError
            raise ValueError(str(ex))


@wraps_type(datetime.datetime)
class DateTime(DefaultableAttribute):
    @staticmethod
    def to_primitive(value, for_db):
        if value is None:
            return None

        if isinstance(value, basestring):
            # make sure this is a valid date
            DateTime.to_python(value)
            return value

        try:
            return value.isoformat()
        except AttributeError as ex:
            raise ValueError(
                "{!r} is not a valid value for DateTime: {}".format(value, ex)
            )

    @staticmethod
    def to_python(value):
        if value is None:
            return None
        try:
            return iso8601.parse_date(value)
        except iso8601.ParseError as ex:
            # ParseError doesn't inherit from ValueError
            raise ValueError(str(ex))


class Choice(DefaultableAttribute):
    choices = Tuple(default=tuple(), required=True)

    def __init__(self, *choices, **kwargs):
        super(Choice, self).__init__(**kwargs)
        self.choices = choices

    # TODO: needs self...
    # @staticmethod
    # def to_primitive(value, for_db):
        # if value is None:
            # return None
        # if value not in self.choices:
            # raise ValueError("Invalid choice `{}`".format(value))
        # return value
