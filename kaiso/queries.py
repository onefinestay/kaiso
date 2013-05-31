from kaiso.relationships import InstanceOf, IsA, DeclaredOn, Defines
from kaiso.types import (
    AttributedBase, get_index_name, Relationship)
from kaiso.serialize import get_type_relationships


def join_lines(*lines, **kwargs):
    rows = []
    sep = kwargs.get('sep', '\n')

    for lne in lines:
        if isinstance(lne, tuple):
            (lne, s) = lne
            lne = '    ' + join_lines(sep=s + '\n    ', *lne)

        if lne != '':
            rows.append(lne)

    return sep.join(rows)


def get_start_clause(obj, name, type_registry):
    """ Returns a node lookup by index as used by the START clause.

    Args:
        obj: An object to create an index lookup.
        name: The name of the object in the query.
    Returns:
        A string with index lookup of a cypher START clause.
    """

    index = next(type_registry.get_index_entries(obj), None)
    if isinstance(obj, Relationship):
        index_type = "rel"
    else:
        index_type = "node"
    query = '{}={}:{}({}="{}")'.format(name, index_type, *index)
    return query


def get_create_types_query(cls, root, type_registry):
    """ Returns a CREATE UNIQUE query for an entire type hierarchy.

    Includes statements that create each type's attributes.

    Args:
        cls: An object to create a type hierarchy for.

    Returns:
        A tuple containing:
        (cypher query, classes to create nodes for, the object names).
    """
    lines = []
    classes = {}

    query_args = {
        'root_id': root.id,
        'IsA_props': type_registry.object_to_dict(IsA()),
        'Defines_props': type_registry.object_to_dict(Defines()),
        'InstanceOf_props': type_registry.object_to_dict(InstanceOf()),
        'DeclaredOn_props': type_registry.object_to_dict(DeclaredOn()),
    }

    # filter type relationships that we want to persist
    type_relationships = []
    for cls1, rel_cls, cls2 in get_type_relationships(cls):
        if issubclass(cls2, AttributedBase):
            type_relationships.append((cls1, rel_cls, cls2))

    # process type relationships
    is_first = True
    for cls1, rel_cls, cls2 in type_relationships:

        name1 = cls1.__name__

        if name1 in classes:
            abstr1 = '`%s`' % (name1,)
        else:
            abstr1 = '(`%s` {%s_props})' % (name1, name1)

        classes[name1] = cls1

        if is_first:
            is_first = False
            ln = 'root -[:DEFINES {Defines_props}]-> %s' % abstr1
        else:
            name2 = cls2.__name__
            classes[name2] = cls2

            rel_name = rel_cls.__name__
            rel_type = rel_name.upper()

            ln = '%s -[:%s {%s_props}]-> %s' % (
                abstr1, rel_type, rel_name, name2)
        lines.append(ln)

    # process attributes
    for name, cls in classes.items():

        descriptor = type_registry.get_descriptor(cls)
        attributes = descriptor.declared_attributes
        for attr_name, attr in attributes.iteritems():
            key = "%s_%s" % (name, attr_name)

            ln = '({%s}) -[:DECLAREDON {DeclaredOn_props}]-> %s' % (
                key, name)
            lines.append(ln)

            attr_dict = type_registry.object_to_dict(
                attr, for_db=True)

            attr_dict['name'] = attr_name
            query_args[key] = attr_dict

    for key, cls in classes.iteritems():
        query_args['%s_props' % key] = type_registry.object_to_dict(cls)

    quoted_names = ('`{}`'.format(cls) for cls in classes.keys())
    query = join_lines(
        'START root=node:%s(id={root_id})' % get_index_name(type(root)),
        'CREATE UNIQUE',
        (lines, ','),
        'RETURN %s' % ', '.join(quoted_names)
    )
    return query, classes.values(), query_args


def get_create_relationship_query(rel, type_registry):
    rel_props = type_registry.object_to_dict(rel, for_db=True)
    query = 'START %s, %s CREATE n1 -[r:%s {props}]-> n2 RETURN r'

    query = query % (
        get_start_clause(rel.start, 'n1', type_registry),
        get_start_clause(rel.end, 'n2', type_registry),
        rel_props['__type__'].upper(),
    )

    return query
