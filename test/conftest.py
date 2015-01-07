import logging
import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--neo4j_uri", action="store",
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

    def make_manager(**kwargs):
        return Manager(neo4j_uri, **kwargs)

    return make_manager


@pytest.fixture
def manager(request, manager_factory):
    manager_factory(skip_setup=True).destroy()
    _manager = manager_factory()
    return _manager


@pytest.fixture
def connection(request):
    from kaiso.persistence import get_connection

    neo4j_uri = request.config.getoption('neo4j_uri')
    return get_connection(neo4j_uri)


@pytest.fixture
def type_registry(request):
    from kaiso.types import TypeRegistry
    registry = TypeRegistry()
    return registry


@pytest.fixture(autouse=True)
def temporary_static_types(request):
    from kaiso.test_helpers import TemporaryStaticTypes

    # need to import these before "freezing" the list of static types
    from kaiso.persistence import TypeSystem
    TypeSystem  # pyflakes

    patcher = TemporaryStaticTypes()
    patcher.start()
    request.addfinalizer(patcher.stop)
