from kaiso.exceptions import TypeNotPersistedError


def test_str():
    try:
        raise TypeNotPersistedError("foo")
    except Exception as ex:
        assert "foo" in str(ex)
