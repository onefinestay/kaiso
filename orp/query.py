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

# MJB: I'd prefer to call argument ``data`` or ``data_dict`` to avoid the
# MJB: railing underscore (and the likelihood of forgetting it)
def encode_query_values(dict_):
    return {k: encode_value(v) for k, v in dict_.items()}
