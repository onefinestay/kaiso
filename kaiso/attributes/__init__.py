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


class PrimitiveTypeMixin(object):
    """ Add a basic `to_primitive` method, coercing value to
    `cls.primitive_type` unless it is `None`.
    """

    @classmethod
    def to_primitive(cls, value, for_db):
        if value is None:
            return None

        return cls.primitive_type(value)


@wraps_type(uuid.UUID)
class Uuid(PrimitiveTypeMixin, Attribute):
    primitive_type = str

    @property
    def default(self):
        return uuid.uuid4()

    @classmethod
    def to_python(cls, value):
        if value is None:
            return None
        return uuid.UUID(hex=value)


class Bool(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = bool


class Integer(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = int


class Float(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = float


class String(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = unicode


@wraps_type(tuple)
class Tuple(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = list

    @classmethod
    def to_python(cls, value):
        if value is None:
            return None
        return tuple(value)


@wraps_type(decimal.Decimal)
class Decimal(PrimitiveTypeMixin, DefaultableAttribute):
    primitive_type = str

    @classmethod
    def to_python(cls, value):
        if value is None:
            return None
        try:
            return decimal.Decimal(value)
        except decimal.DecimalException as ex:
            # DecimalException doesn't inherit from ValueError
            raise ValueError(str(ex))


@wraps_type(datetime.datetime)
class DateTime(DefaultableAttribute):
    @classmethod
    def to_primitive(cls, value, for_db):
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

    @classmethod
    def to_python(cls, value):
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
