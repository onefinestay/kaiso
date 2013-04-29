import os
import subprocess
import shutil

from mock import patch
import pytest

import kaiso.connection
from kaiso.connection import get_connection, TempConnectionError


@pytest.mark.slow
class TestTempConnectionProcesses():
    """ Test spinning up temporary neo4j processes.

    We mock out ``py2neo.neo4j.GraphDatabaseService`` because:
        a) it's a library and we're not testing its behaviour directly
        b) the first thing GraphDatabaseService does is generate a
           GET request (the same one we do in ``get_connection``)
           whch sometimes results in a SocketError. This is possibly
           a bug in neo4j, but it shouldn't influence these tests.
    """
    temp_data_dir = '/tmp/foo'

    def teardown_method(self, method):
        """ Kill process and remove temp_data_dir after every test.
        """
        for key, proc in kaiso.connection._temporary_databases.items():
            proc.terminate()
            proc.wait()
            del kaiso.connection._temporary_databases[key]
        if os.path.exists(self.temp_data_dir):
            shutil.rmtree(self.temp_data_dir)

    def test_temp_connection_defaults(self):
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://')
            assert conn == "http://localhost:7475/db/data/"

    def test_temp_connection_custom_port(self):
        port = "7777"
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(port))
            assert conn == "http://localhost:{}/db/data/".format(port)

    def test_temp_connection_custom_data_dir(self):
        data_dir = self.temp_data_dir
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(data_dir))
            assert conn == "http://localhost:7475/db/data/"
            assert os.path.exists(data_dir)

    def test_temp_connection_custom(self):
        port = "7777"
        data_dir = self.temp_data_dir

        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}{}'.format(port, data_dir))
            assert conn == "http://localhost:{}/db/data/".format(port)
            assert os.path.exists(data_dir)

    def test_multiple_temp_connections(self):

        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn1 = get_connection('temp://')
            with patch.object(kaiso.connection, 'get_neo4j_info') as get_info:
                conn2 = get_connection('temp://')
                assert conn1 == conn2
                assert not get_info.called  # we should be reusing existing db

    def test_temp_connection_timeout(self):
        with patch.object(kaiso.connection, 'TIMEOUT', 0):
            with pytest.raises(TempConnectionError):
                get_connection('temp://8888')


def test_temp_connection_no_neo4j_info():
    with patch.object(subprocess, 'check_output', lambda _: None):
        with pytest.raises(TempConnectionError):
            get_connection('temp://8888')
