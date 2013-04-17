import os

from orp.connection import get_connection


def test_temp_connection_defaults():
    conn = get_connection('temp://')
    assert conn == "http://localhost:7475/db/data/"


def test_temp_connection_custom_port():
    port = "7777"
    conn = get_connection('temp://{}'.format(port))
    assert conn == "http://localhost:{}/db/data/".format(port)


def test_temp_connection_custom_data_dir():
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}'.format(data_dir))
    assert conn == "http://localhost:7475/db/data/"
    assert os.path.exists(data_dir)


def test_temp_connection_custom():
    port = "7777"
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}{}'.format(port, data_dir))
    assert conn == "http://localhost:{}/db/data/".format(port)
    assert os.path.exists(data_dir)
