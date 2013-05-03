import os
import shutil

from mock import patch
import pytest
import socket
import requests

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
        for key, temp_neo4j in kaiso.connection._temporary_databases.items():
            temp_neo4j.process.terminate()
            temp_neo4j.process.wait()
            del kaiso.connection._temporary_databases[key]
        if os.path.exists(self.temp_data_dir):
            shutil.rmtree(self.temp_data_dir)

    def test_temp_connection_defaults(self):
        """ Verify temporary connection with default options.
        """
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://')
        assert conn == "http://127.0.0.1:7475/db/data/"
        assert "neo4j_version" in requests.get(conn).text

    def test_temp_connection_custom_port(self):
        """ Verify temporary connection on a custom port.
        """
        port = "7777"
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://:{}'.format(port))
        assert conn == "http://127.0.0.1:{}/db/data/".format(port)
        assert "neo4j_version" in requests.get(conn).text

    def test_temp_connection_custom_data_dir(self):
        """ Verify temporary connection with a custom data directory.
        """
        data_dir = self.temp_data_dir
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(data_dir))
        assert conn == "http://127.0.0.1:7475/db/data/"
        assert "neo4j_version" in requests.get(conn).text
        assert os.path.exists(data_dir)

    def test_temp_connection_custom_bind_iface(self):
        """ Verify temporary connection with bound to a specific interface.
        """
        bind_iface = "0.0.0.0"
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(bind_iface))

        assert conn == "http://{}:7475/db/data/".format(bind_iface)
        # ensure we can reach the webservice via an external hostname
        hostname = socket.gethostname()
        external_url = "http://{}:7475/db/data/".format(hostname)
        assert "neo4j_version" in requests.get(external_url).text

    def test_temp_connection_custom(self):
        """ Verify temporary connection with custom everything.
        """
        port = "7777"
        bind_iface = "0.0.0.0"
        data_dir = self.temp_data_dir

        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}:{}{}'.format(bind_iface, port,
                                                          data_dir))
        assert conn == "http://{}:{}/db/data/".format(bind_iface, port)
        assert os.path.exists(data_dir)
        # ensure we can reach the webservice via an external hostname
        hostname = socket.gethostname()
        external_url = "http://{}:{}/db/data/".format(hostname, port)
        assert "neo4j_version" in requests.get(external_url).text

    def test_multiple_temp_connections(self):
        """ Verify that temporary connections are recycled if the uri matches
        """
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn1 = get_connection('temp://')
            with patch.object(kaiso.connection, 'get_neo4j_info') as get_info:
                conn2 = get_connection('temp://')
                assert conn1 == conn2
                assert not get_info.called  # we should be reusing existing db

    def test_temp_connection_timeout(self):
        """ Verify that an exception is raised if the process times out.
        """
        with patch.object(kaiso.connection, 'TIMEOUT', 0):
            with pytest.raises(TempConnectionError):
                get_connection('temp://8888')
