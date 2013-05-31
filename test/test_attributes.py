from kaiso.attributes import Uuid, Tuple


def test_ignores_none():
    assert Uuid.to_python(None) is None
    assert Tuple.to_python(None) is None
