from kaiso.attributes import (
    Bool, Choice, DateTime, Decimal, Float, Integer, String, Tuple, Uuid)


def test_primitive_ignores_none():
    assert Bool.to_primitive(None, True) is None
    assert Choice.to_primitive(None, True) is None
    assert DateTime.to_primitive(None, True) is None
    assert Decimal.to_primitive(None, True) is None
    assert Float.to_primitive(None, True) is None
    assert Integer.to_primitive(None, True) is None
    assert String.to_primitive(None, True) is None
    assert Tuple.to_primitive(None, True) is None
    assert Uuid.to_primitive(None, True) is None
    assert Uuid.to_primitive(None, True) is None


def test_python_ignores_none():
    assert Bool.to_python(None) is None
    assert Choice.to_python(None) is None
    assert DateTime.to_python(None) is None
    assert Decimal.to_python(None) is None
    assert Float.to_python(None) is None
    assert Integer.to_python(None) is None
    assert String.to_python(None) is None
    assert Tuple.to_python(None) is None
    assert Uuid.to_python(None) is None
    assert Uuid.to_python(None) is None
