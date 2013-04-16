from orp.connection import get_connection


def test_temp_connection():
    conn = get_connection('temp://7475')
    assert conn
