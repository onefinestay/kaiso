import os
import re
import time
import atexit
import logging
import requests
import tempfile
import subprocess

from py2neo import neo4j

logger = logging.getLogger(__name__)

_temporary_databases = {}

TIMEOUT = 30  # seconds


def get_neo4j_info():
    ''' Gets runtime information from the neo4j command.

    Returns:
        A dict.
    '''
    output = subprocess.check_output(['neo4j', 'info'])

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


def temp_neo4j_instance(uri):
    """
    Start or return an existing instance of the neo4j graph database server
    using the given URI
    """

    # split the uri
    match = re.match(r"temp://?(?P<port>\d*)(?P<temp_dir>.*)", uri)

    neo4j_info = get_neo4j_info()

    port = match.group("port") or '7475'
    temp_dir = match.group("temp_dir") or tempfile.mkdtemp()
    temp_data_dir = os.path.join(temp_dir, 'data')
    if not os.path.exists(temp_data_dir):
        os.makedirs(temp_data_dir)

    # return http uri if this temporary database already exists,
    # using the port as the unique identifer
    if port in _temporary_databases:
        return "http://localhost:{}/db/data/".format(port)

    # otherwise, start a new neo4j process.
    # define the subprocess command and the required classpath
    cmd = ['java', '-cp']
    cmd.append(neo4j_info['CLASSPATH'])

    # neo requires a physical config file which we temporarily create to
    # specify config overrides
    config_options = {
        'org.neo4j.server.webserver.port': port,
        'org.neo4j.server.database.location': temp_data_dir
    }

    props_filepath = os.path.join(temp_dir, 'server.properties')
    with open(props_filepath, 'w') as props_file:
        for key, value in config_options.iteritems():
            props_file.write("{}={}\n".format(key, value))

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
    }
    for key, value in startup_options.iteritems():
        args.append("-D{}={}".format(key, value))

    # finally add the class to run
    args.append('org.neo4j.server.Bootstrapper')
    cmd.extend(args)

    # start the subprocess
    neo4j_process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)

    _temporary_databases[port] = neo4j_process

    # terminate subprocess at exit
    atexit.register(neo4j_process.terminate)

    # the startup process is async so we monitor the http interface to know
    # when to allow the test runner to continue
    started = time.time()
    url = "http://localhost:{}/db/data/".format(port)

    while time.time() < started + TIMEOUT:
        try:
            req = requests.get(url)
            if "neo4j_version" in req.text:
                logger.debug('neo4j server started on {}'.format(url))
                return url  # return REST API url
        except requests.ConnectionError:
            time.sleep(0.2)

    logger.critical('Unable to start Neo4j: timeout after {} '
                    'seconds. See logs in {}.'.format(TIMEOUT, temp_data_dir))


def get_connection(uri):
    if uri.startswith('temp://'):
        uri = temp_neo4j_instance(uri)
    graph_db = neo4j.GraphDatabaseService(uri)
    return graph_db
