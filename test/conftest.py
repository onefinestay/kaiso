import os
import pytest

import logging


def pytest_addoption(parser):
    parser.addoption(
        "--neo4j_uri", action="store",
        default="temp://",
        help=("URI for establishing a connection to neo4j."
        "See the docs for valid URIs"))

    parser.addoption(
        "--neo4j_cmd", action="store",
        help=("Location of neo4j script that provides installation 'info'"))

    parser.addoption(
        "--log-level", action="store",
        default=None,
        help=("The logging-level for the test run."))


def pytest_configure(config):
    neo4j_cmd = config.getoption('neo4j_cmd')
    if neo4j_cmd:
        os.environ['NEO4J_CMD'] = neo4j_cmd

    log_level = config.getoption('log_level')
    if log_level is not None:
        logging.basicConfig(level=getattr(logging, log_level))
        logging.getLogger('py2neo').setLevel(logging.ERROR)


@pytest.fixture
def manager_factory(request):
    from kaiso.persistence import Manager

    neo4j_uri = request.config.getoption('neo4j_uri')
    def make_manager():
        return Manager(neo4j_uri)

    return make_manager


@pytest.fixture
def manager(request, manager_factory):
    manager_factory().destroy()
    _manager = manager_factory()
    return _manager


@pytest.fixture
def type_registry(request):
    from kaiso.types import TypeRegistry
    registry = TypeRegistry()
    return registry
