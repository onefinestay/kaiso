import pytest

from kaiso.exceptions import UnknownType


def test_unknown_type(type_registry):
    with pytest.raises(UnknownType):
        type_registry.get_descriptor(UnknownType)
