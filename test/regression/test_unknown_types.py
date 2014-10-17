import pytest

from kaiso.attributes import Integer, String
from kaiso.types import Entity


fixture = pytest.mark.usefixtures('storage')


def test_skip_setup(manager, manager_factory):
    # Type loading will fail if the database contains references to an unknown
    # attribute or base (i.e. __type__ refers to a class not known by Python).
    # Test we can skip it when creating a manager. (e.g. for the one we use to
    # call ``destroy``)

    class Unknown(Entity):
        id = Integer(default=1)
        unknown_attr = String()

    manager.save(Unknown)
    result = manager._execute("""
        START n=node(*)
        WHERE n.name = "unknown_attr"
        SET n.__type__ = "UnknownAttribute";
    """)

    # force query evaluation
    list(result)

    # should succeed
    manager_factory(skip_setup=True)
