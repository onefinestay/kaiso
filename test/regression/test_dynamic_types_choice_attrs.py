from kaiso.attributes import String, Choice
from kaiso.types import Entity


def test_get_create_types_query(manager):

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
