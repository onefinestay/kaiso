import pytest

from kaiso.attributes import String
from kaiso.exceptions import NoUniqueAttributeError
from kaiso.queries import get_start_clause, get_match_clause
from kaiso.types import Entity, Relationship, TypeRegistry


class IndexableThing(Entity):
    indexable_attr = String(unique=True)


class TwoUniquesThing(IndexableThing):
    also_unique = String(unique=True)


class Connects(Relationship):
    indexable_attr = String(unique=True)


class NotIndexable(Entity):
    pass


type_registry = TypeRegistry()


def test_get_start_clause_for_type():
    clause = get_start_clause(IndexableThing, "foo", type_registry)
    assert clause == 'foo=node:persistabletype(id="IndexableThing")'


def test_get_start_clause_for_instance():
    obj = IndexableThing(indexable_attr="bar")

    clause = get_start_clause(obj, "foo", type_registry)
    assert clause == 'foo=node:indexablething(indexable_attr="bar")'


def test_get_start_clause_mutiple_uniques():
    obj = TwoUniquesThing(
        indexable_attr="bar",
        also_unique="baz"
    )

    clause = get_start_clause(obj, "foo", type_registry)
    assert (clause == 'foo=node:indexablething(indexable_attr="bar")' or
            clause == 'foo=node:indexablething(also_unique="baz")')


def test_get_start_clause_for_relationship_type():
    clause = get_start_clause(Connects, "foo", type_registry)
    assert clause == 'foo=node:persistabletype(id="Connects")'


def test_get_start_clause_for_relationship_instance():
    a = IndexableThing()
    b = IndexableThing()

    obj = Connects(start=a, end=b, indexable_attr="bar")

    clause = get_start_clause(obj, "foo", type_registry)
    assert clause == 'foo=rel:connects(indexable_attr="bar")'


def test_get_start_clause_no_uniques():
    with pytest.raises(NoUniqueAttributeError):
        get_start_clause(NotIndexable(), 'foo', type_registry)


def test_get_match_clause_for_type():
    clause = get_match_clause(IndexableThing, "foo", type_registry)
    assert clause == '(foo:PersistableType {id: "IndexableThing"})'


def test_get_match_clause_for_instance():
    obj = IndexableThing(indexable_attr="bar")

    clause = get_match_clause(obj, "foo", type_registry)
    assert clause == '(foo:IndexableThing {indexable_attr: "bar"})'


def test_get_match_clause_mutiple_uniques():
    obj = TwoUniquesThing(
        indexable_attr="bar",
        also_unique="baz"
    )

    clause = get_match_clause(obj, "foo", type_registry)
    assert (clause == '(foo:IndexableThing {indexable_attr: "bar"})' or
            clause == '(foo=IndexableThing {also_unique: "baz"})')


def test_get_match_clause_no_uniques():
    with pytest.raises(NoUniqueAttributeError):
        get_match_clause(NotIndexable(), 'foo', type_registry)


def test_get_match_clause_bad_type():
    with pytest.raises(ValueError) as exc:
        get_match_clause(object(), 'foo', type_registry)
    assert "only supported for Entities" in str(exc)
