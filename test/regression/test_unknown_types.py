import pytest

from kaiso.attributes import Integer, String
from kaiso.types import Entity


fixture = pytest.mark.usefixtures('storage')


def test_unknown_attr(manager, manager_factory):

    class Unknown(Entity):
        id = Integer(default=1)
        unknown_attr = String()

    manager.save(Unknown)
    result = manager._execute("""
        START n=node(*)
        WHERE n.name! = "unknown_attr"
        SET n.__type__ = "UnknownAttribute";
    """)

    next(result)

    # should succeed
    manager_factory()
