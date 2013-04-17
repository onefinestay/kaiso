import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--neo4j_uri", action="store",
        default='http://localhost:7474/db/data',  # TODO: -> temp://
        help=("URI for establishing a connection to neo4j."
        "See the docs for valid URIs"))


@pytest.fixture
def storage(request):
    # to make sure this doesn't run before coverage: TODO check if we need this
    from orp.persistence import Storage
    neo4j_uri = request.config.getoption('neo4j_uri')
    storage = Storage(neo4j_uri)
    storage.clear()
    return storage
