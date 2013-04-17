import os

from mock import patch
import pytest

from orp.connection import get_connection, TempConnectionError


def test_temp_connection_defaults():
    conn = get_connection('temp://')
    assert conn.__uri__ == "http://localhost:7475/db/data/"


def test_temp_connection_custom_port():
    port = "7777"
    conn = get_connection('temp://{}'.format(port))
    assert conn.__uri__ == "http://localhost:{}/db/data/".format(port)


def test_temp_connection_custom_data_dir():
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}'.format(data_dir))
    assert conn.__uri__ == "http://localhost:7475/db/data/"
    assert os.path.exists(data_dir)


def test_temp_connection_custom():
    port = "7777"
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}{}'.format(port, data_dir))
    assert conn.__uri__ == "http://localhost:{}/db/data/".format(port)
    assert os.path.exists(data_dir)


def test_temp_connection_timeout():
    with patch('orp.connection.TIMEOUT', new_callable=lambda: 0):
        with pytest.raises(TempConnectionError):
            get_connection('temp://8888')



