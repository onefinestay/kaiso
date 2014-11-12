import pytest

from kaiso.attributes import String, Choice
from kaiso.types import Entity


"""
bug in the neo4j rest interface when using params:

# starting with an empty db, we first create a starting point
CYPHER="http://localhost:7474/db/data/cypher"
curl \
    -i \
    -H "Content-Type: application/json" \
    -d '{"query": "CREATE (n:start) RETURN n"}' \
    $CYPHER

# create unique without parameters
curl \
    -i \
    -H "Content-Type: application/json" \
    -d '{"query": \
        "MATCH (s:start) \
        CREATE UNIQUE s-[:REL]->(n:label {foo: [1]}) \
        RETURN n"}' \
    $CYPHER

# repeating this query we only get a single new node created
# however, using parameters instead
# (this only seems to affect `list` properties)

curl \
    -i \
    -H "Content-Type: application/json" '
    -d '{"query": \
        "MATCH (s:start) \
        CREATE UNIQUE s-[:REL]->(n:label {param}) \
        RETURN n", "params": {"param": {"foo": [1]}}}' \
    $CYPHER

# repeating this query, each call creates a new node
"""


@pytest.mark.xfail
def test_foo(manager):

    attrs = {
        'id': String(unique=True),
        'choice': Choice(),
    }
    Foo = manager.create_type('Foo', (Entity,), attrs)
    SubFoo = manager.create_type('SubFoo', (Foo,), {})

    manager.save(Foo)
    manager.save(SubFoo)

    rows = manager.query("""
        MATCH (Foo:PersistableType {id: "Foo"})<-[:DECLAREDON]-(attr)
        WHERE attr.name = "choice"
        RETURN attr
    """)

    assert len(list(rows)) == 1
