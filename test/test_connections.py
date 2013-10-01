import os
import socket
import subprocess
import shutil

from mock import patch, ANY
import pytest

import kaiso.connection
from kaiso.connection import get_connection, write_config
from kaiso.exceptions import ConnectionError


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
    temp_dir = '/tmp/foo'

    def setup_method(self, method):
        # patch write_config to itself so we can assert arguments
        self._p_write_config = patch('kaiso.connection.write_config')
        self.write_config = self._p_write_config.start()
        self.write_config.side_effect = write_config

        self._original_temporary_databases = (
            kaiso.connection._temporary_databases.copy())
        kaiso.connection._temporary_databases = {}

    def teardown_method(self, method):
        # kill processes
        for key, temp_neo4j in kaiso.connection._temporary_databases.items():
            temp_neo4j.process.terminate()
            temp_neo4j.process.wait()
            del kaiso.connection._temporary_databases[key]
        # remove temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        # stop patch
        self._p_write_config.stop()

        kaiso.connection._temporary_databases = (
            self._original_temporary_databases)

    def test_temp_connection_defaults(self):
        """ Verify temporary connection with default options.
        """
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            # make sure we don't clash with the default temp port
            with patch('kaiso.connection.DEFAULT_TEMP_PORT', 7776):
                conn = get_connection('temp://')

        assert conn == "http://localhost:7776/db/data/"
        self.write_config.assert_called_once_with(
            ANY, 7776, ANY, 'localhost'
        )

    def test_temp_connection_custom_port(self):
        """ Verify temporary connection on a custom port.
        """
        port = "7777"
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://:{}'.format(port))

        assert conn == "http://localhost:{}/db/data/".format(port)
        self.write_config.assert_called_once_with(
            ANY, 7777, ANY, 'localhost'
        )

    def test_temp_connection_custom_data_dir(self):
        """ Verify temporary connection with a custom data directory.
        """
        temp_dir = self.temp_dir
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(temp_dir))

        assert conn == "http://localhost:7475/db/data/"
        assert os.path.exists(temp_dir)
        self.write_config.assert_called_once_with(
            os.path.join(temp_dir, 'server.properties'), 7475,
            os.path.join(temp_dir, 'data'), 'localhost'
        )

    def test_temp_connection_custom_bind_iface(self):
        """ Verify temporary connection with bound to a specific interface.
        """
        bind_iface = "0.0.0.0"
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}'.format(bind_iface))

        assert conn == "http://localhost:7475/db/data/"
        self.write_config.assert_called_once_with(
            ANY, 7475, ANY, bind_iface
        )

    def test_temp_connection_custom(self):
        """ Verify temporary connection with custom everything.
        """
        port = "7777"
        bind_iface = socket.gethostname().lower()
        temp_dir = self.temp_dir
        with patch('py2neo.neo4j.GraphDatabaseService', lambda uri: uri):
            conn = get_connection('temp://{}:{}{}'.format(bind_iface, port,
                                                          temp_dir))

        assert conn == "http://{}:{}/db/data/".format(bind_iface, port)
        assert os.path.exists(temp_dir)
        self.write_config.assert_called_once_with(
            os.path.join(temp_dir, 'server.properties'), 7777,
            os.path.join(temp_dir, 'data'), bind_iface
        )

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
            with pytest.raises(ConnectionError):
                get_connection('temp://8888')


def test_temp_connection_no_neo4j_info():
    """ Verify that we throw an error if neo4j info cannot be determined.
    """
    with patch.object(subprocess, 'check_output') as check_output:
        check_output.side_effect = OSError()
        with pytest.raises(ConnectionError):
            get_connection('temp://8888')


def test_temp_connection_empty_string():
    """ Verify that we throw an error if an empty connection string is used.
    """
    with pytest.raises(ConnectionError):
        get_connection('')
