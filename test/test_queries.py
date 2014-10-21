# coding: utf-8
from __future__ import unicode_literals

from itertools import permutations
from textwrap import dedent

import pytest

from kaiso.attributes import String
from kaiso.exceptions import NoUniqueAttributeError
from kaiso.queries import get_match_clause, parameter_map, inline_parameter_map
from kaiso.types import Entity, Relationship, TypeRegistry


class IndexableThing(Entity):
    indexable_attr = String(unique=True)


class TwoUniquesThing(IndexableThing):
    also_unique = String(unique=True)


class Connects(Relationship):
    pass


class NotIndexable(Entity):
    pass


type_registry = TypeRegistry()


def test_parameter_map():
    assert parameter_map({'foo': 'bar', 'baz': 'ಠ_ಠ'}, "params") == (
        '{foo: {params}.foo, baz: {params}.baz}'
    )


def test_inline_parameter_map():
    assert inline_parameter_map({'foo': 'bar', 'baz': 'ಠ_ಠ'}) == (
        '{foo: "bar", baz: "\\u0ca0_\\u0ca0"}'
    )


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

    match_clause = get_match_clause(obj, "foo", type_registry)
    # order if labels and properties are undefined, so try all possibilities
    possible_labels = ['IndexableThing', 'TwoUniquesThing']
    possible_attrs = ['indexable_attr: "bar"', 'also_unique: "baz"']
    possible_clauses = set()
    for labels in permutations(possible_labels, 2):
        for attrs in permutations(possible_attrs, 2):
            clause = '(foo:{} {{{}}})'.format(
                ':'.join(labels), ', '.join(attrs)
            )
            possible_clauses.add(clause)

    assert match_clause in possible_clauses


def test_get_match_clause_no_uniques():
    with pytest.raises(NoUniqueAttributeError):
        get_match_clause(NotIndexable(), 'foo', type_registry)


def test_get_match_clause_bad_unique_value():
    with pytest.raises(NoUniqueAttributeError):
        get_match_clause(IndexableThing(
            indexable_attr=None), 'foo', type_registry)


def test_get_match_clause_for_relationship():
    a = IndexableThing(indexable_attr='a')
    b = IndexableThing(indexable_attr='b')
    rel = Connects(start=a, end=b)
    match_clause = get_match_clause(rel, 'rel', type_registry)
    expected = """
        (rel__start:IndexableThing {indexable_attr: "a"}),
        (rel__end:IndexableThing {indexable_attr: "b"}),
        (rel__start)-[rel:CONNECTS]->(rel__end)
    """
    assert match_clause == dedent(expected)


def test_get_match_clause_for_relationship_missing_endpoint():
    rel = Connects()
    with pytest.raises(NoUniqueAttributeError) as exc:
        get_match_clause(rel, 'rel', type_registry)
    assert 'is missing a start or end node' in str(exc)


@pytest.mark.parametrize('cls', (
    NotIndexable,
    IndexableThing,  # indexable, but no value set
))
def test_get_match_clause_for_relationship_non_unique_endpoint(cls):
    a = cls()
    b = cls()

    rel = Connects(start=a, end=b)
    with pytest.raises(NoUniqueAttributeError) as exc:
        get_match_clause(rel, 'rel', type_registry)
    assert "doesn't have any unique attributes" in str(exc)
