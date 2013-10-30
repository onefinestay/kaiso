""" Provides a connection factory for connecting to existing or
temporary neo4j instances.

For temporary instances it requires the neo4j command
to be available on the path.
"""
import os
import time
import atexit
import logging
import requests
import tempfile
import urlparse
import subprocess

from collections import namedtuple

from py2neo import neo4j

from kaiso.exceptions import ConnectionError

logger = logging.getLogger(__name__)

temp_neo4j = namedtuple('temp_neo4j', ['http_url', 'process'])
_temporary_databases = {}

TIMEOUT = 30  # seconds
DEFAULT_TEMP_PORT = 7475


def get_neo4j_info():
    """ Gets runtime information from the neo4j command.

    Returns:
        A dict.
    """
    default_cmd = os.pathsep.join([
        'neo4j',
        '/var/lib/neo4j/bin/neo4j',  # >= 1.9.4
        '/etc/init.d/neo4j-service',
    ])
    neo4j_cmds = os.environ.get('NEO4J_CMD', default_cmd).split(os.pathsep)
    output = None
    for cmd in neo4j_cmds:
        try:
            output = subprocess.check_output([cmd, 'info'])
        except (OSError, subprocess.CalledProcessError):
            pass

    if not output:
        raise ConnectionError('Cannot determine neo4j info. Is the NEO4J_CMD '
                              'environment varaible set correctly?')

    keys = ['NEO4J_HOME', 'NEO4J_INSTANCE', 'JAVA_OPTS', 'CLASSPATH']

    result = {}

    for lne in output.splitlines():
        for key in keys:
            sep = key + ':'
            if lne.startswith(sep):
                _, value = lne.split(sep)
                value = value.strip()
                result[key] = value

    return result


def write_config(config_filepath, port, temp_data_dir, bind_iface):
    """ Write neo4j server properties into the file at ``config_filepath``
    """
    config_options = {
        'org.neo4j.server.webserver.port': port,
        'org.neo4j.server.database.location': temp_data_dir,
        'org.neo4j.server.webserver.address': bind_iface
    }

    with open(config_filepath, 'w') as props_file:
        for key, value in config_options.iteritems():
            props_file.write("{}={}\n".format(key, value))


def temp_neo4j_instance(uri):
    """ Start or return an existing instance of the neo4j graph database server
    using the given URI.
    """
    # split the uri
    split_uri = urlparse.urlparse(uri)
    port = split_uri.port or DEFAULT_TEMP_PORT
    bind_iface = split_uri.hostname or "localhost"

    # return http uri if this temporary database already exists
    if uri in _temporary_databases:
        return _temporary_databases[uri].http_url

    neo4j_info = get_neo4j_info()

    temp_dir = split_uri.path or tempfile.mkdtemp()
    temp_data_dir = os.path.join(temp_dir, 'data')
    if not os.path.exists(temp_data_dir):
        os.makedirs(temp_data_dir)

    # otherwise, start a new neo4j process.
    # define the subprocess command and the required classpath
    cmd = ['java', '-cp']
    cmd.append(neo4j_info['CLASSPATH'])

    # neo requires a physical config file which we temporarily create to
    # specify config overrides
    props_filepath = os.path.join(temp_dir, 'server.properties')
    write_config(props_filepath, port, temp_data_dir, bind_iface)

    # required startup args
    args = [
        '-server', '-XX:+DisableExplicitGC'
    ]

    # additional startup options for the command line
    startup_options = {
        'neo4j.home': neo4j_info['NEO4J_HOME'],
        'neo4j.instance': neo4j_info['NEO4J_INSTANCE'],
        'org.neo4j.server.properties': props_filepath,
        'file.encoding': 'UTF-8',
        'java.awt.headless': 'true',
    }
    for key, value in startup_options.iteritems():
        args.append("-D{}={}".format(key, value))

    # finally add the class to run
    args.append('org.neo4j.server.Bootstrapper')
    cmd.extend(args)

    # start the subprocess
    neo4j_process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)

    # terminate subprocess at exit
    def terminate():  # pragma: no cover
        try:
            neo4j_process.terminate()
        except OSError:
            pass  # already terminated
    atexit.register(terminate)

    # the startup process is async so we monitor the http interface to know
    # when to allow the test runner to continue
    timeout_time = time.time() + TIMEOUT
    hostname = "localhost" if bind_iface == "0.0.0.0" else bind_iface
    http_url = "http://{}:{}/db/data/".format(hostname, port)

    while time.time() < timeout_time:
        try:
            req = requests.get(http_url)
            if "neo4j_version" in req.text:
                logger.debug('neo4j server started on {}'.format(http_url))
                _temporary_databases[uri] = temp_neo4j(http_url, neo4j_process)
                return http_url  # return REST API url
        except requests.ConnectionError:
            time.sleep(0.2)

    logger.critical(
        'Unable to start Neo4j: timeout after %s '
        'seconds. See logs in %s.', TIMEOUT, temp_data_dir)

    raise ConnectionError(http_url)


def get_connection(uri):
    if not uri:
        raise ConnectionError("You must provide a connection URI")
    if uri.startswith('temp://'):
        uri = temp_neo4j_instance(uri)
    graph_db = neo4j.GraphDatabaseService(uri)
    return graph_db
