import os
import re
import atexit
import logging
import tempfile
import subprocess

from py2neo import neo4j

logger = logging.getLogger(__name__)

_temporary_databases = {}


def build_neo4j_classpath(neo4j_home):
    """
    Collects paths to jar files required by neo4j
    """
    logger.debug('building neo4j classpath from {}'.format(neo4j_home))

    # collect our classpaths
    classpaths = []

    rel_paths = ['lib', 'system/lib']
    for path in rel_paths:
        class_path = os.path.join(neo4j_home, path)
        if not os.path.exists(class_path):
            raise IOError('Neo4j is incorrectly configured {} directory not '
                          'found in neo4j_home ({}).'.format(path, class_path))

        for root, _, files in os.walk(class_path):
            jars = [f for f in files if f.endswith('.jar')]
            for jar in jars:
                classpaths.append(os.path.join(root, jar))

    return ':'.join(classpaths)


def temp_neo4j_instance(uri):
    """
    Start or return an existing instance of the neo4j graph database server
    using the given URI
    """
    # split the uri
    match = re.match("temp://(?P<path>[^:]+):?(?P<port>\d*)(?P<data_dir>.*)",
                     uri)
    neo4j_home = match.group("path")
    port = match.group("port") or '7475'
    temp_data_dir = match.group("data_dir") or tempfile.mkdtemp()

    # return http uri if this temporary database already exists,
    # using the port as the unique identifer
    if port in _temporary_databases:
        return "http://localhost:{}/db/data/".format(port)

    # otherwise, start a new neo4j process.
    # define the subprocess command and the required classpath
    cmd = ['java', '-cp']
    cmd.append(build_neo4j_classpath(neo4j_home))

    # neo requires a physical config file which we temporarily create to
    # specify config overrides
    config_options = {
        'org.neo4j.server.webserver.port': port,
        'org.neo4j.server.database.location': temp_data_dir
    }
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        for key, value in config_options.iteritems():
            tf.write("{}={}\n".format(key, value))

    # required startup args
    args = [
        '-server', '-XX:+DisableExplicitGC'
    ]

    # additional startup options for the command line
    startup_options = {
        'neo4j.home': neo4j_home,
        'neo4j.instance': neo4j_home,
        'org.neo4j.server.properties': tf.name,
        'file.encoding': 'UTF-8',
    }
    for key, value in startup_options.iteritems():
        args.append("-D{}={}".format(key, value))

    # finally add the class to run
    args.append('org.neo4j.server.Bootstrapper')
    cmd.extend(args)

    # start the subprocess
    neo4j_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    _temporary_databases[port] = neo4j_process

    # terminate subprocess at exit
    atexit.register(neo4j_process.terminate)

    # the startup process is async so we monitor stdout to know when to allow
    # the test runner to continue
    for line in neo4j_process.stdout:
        if "Server started" in line:
            break
        elif "SEVERE" in line:
            logger.warning(line)
    else:
        # if the pipe is exhausted, we've failed to start
        logger.critical('Unable to start Neo4j')
        os.exit()

    # clear up & return REST API url
    os.unlink(tf.name)
    url = "http://localhost:{}/db/data/".format(port)
    logger.debug('neo4j server started on {}'.format(url))
    return url


def get_connection(uri):
    if uri.startswith('temp://'):
        uri = temp_neo4j_instance(uri)
    graph_db = neo4j.GraphDatabaseService(uri)
    return graph_db
