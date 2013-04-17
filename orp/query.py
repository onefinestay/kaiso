import datetime
import decimal
import uuid

from orp import attributes

TYPE_MAP = {
    uuid.UUID: attributes.Uuid,
    decimal.Decimal: attributes.Decimal,
    datetime.datetime: attributes.DateTime,
}


def encode_value(value):
    attr_class = TYPE_MAP.get(type(value))

    if attr_class:
        return attr_class.to_db(value)

    return value


def encode_query_values(data):
    return {k: encode_value(v) for k, v in data.items()}
