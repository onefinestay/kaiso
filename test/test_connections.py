import os

import pytest

from orp.connection import get_connection


@pytest.mark.slow
def test_temp_connection_defaults():
    conn = get_connection('temp://')
    assert conn.__uri__ == "http://localhost:7475/db/data/"


@pytest.mark.slow
def test_temp_connection_custom_port():
    port = "7777"
    conn = get_connection('temp://{}'.format(port))
    assert conn.__uri__ == "http://localhost:{}/db/data/".format(port)


@pytest.mark.slow
def test_temp_connection_custom_data_dir():
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}'.format(data_dir))
    assert conn.__uri__ == "http://localhost:7475/db/data/"
    assert os.path.exists(data_dir)


@pytest.mark.slow
def test_temp_connection_custom():
    port = "7777"
    data_dir = '/tmp/foo'

    conn = get_connection('temp://{}{}'.format(port, data_dir))
    assert conn.__uri__ == "http://localhost:{}/db/data/".format(port)
    assert os.path.exists(data_dir)
