import os
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--neo4j_uri", action="store",
        default='http://localhost:7474/db/data',  # TODO: -> temp://
        help=("URI for establishing a connection to neo4j."
        "See the docs for valid URIs"))

    parser.addoption(
        "--neo4j_cmd", action="store",
        help=("Location of neo4j script that provides installation 'info'"))


def pytest_configure(config):
    neo4j_cmd = config.getoption('neo4j_cmd')
    if neo4j_cmd:
        os.environ['NEO4J_CMD'] = neo4j_cmd


@pytest.fixture
def storage(request):
    # to make sure this doesn't run before coverage: TODO check if we need this
    from kaiso.persistence import Storage

    neo4j_uri = request.config.getoption('neo4j_uri')
    storage = Storage(neo4j_uri)
    storage.delete_all_data()
    storage.initialize()
    return storage
