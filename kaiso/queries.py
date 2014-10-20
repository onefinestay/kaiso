import json
from textwrap import dedent

from kaiso.exceptions import NoUniqueAttributeError
from kaiso.relationships import InstanceOf, IsA, DeclaredOn, Defines
from kaiso.serialize import object_to_db_value, dict_to_db_values_dict
from kaiso.types import (
    AttributedBase, Relationship, get_type_id, get_relationship_id,
    PersistableType)
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


def parameter_map(data, name):
    """Convert a dict of paramters into a "parameter map", as parameter dicts
    cannot be used in e.g. MATCH patterns

    Example:
        >>> parameter_map({'foo': 'bar'}, "params")
        {foo: {params}.foo}
    """
    return "{%s}" % (
        ', '.join("%s: {%s}.%s" % (key, name, key)
        for key in data
    ))


def inline_parameter_map(data):
    """Convert a dict of paramters into a "parameter map" to be used in line
    (not using parameter substitution) in e.g. a match clause. Unline e.g.
    json, they keys are not quoted

    Example:
        >>> inline_parameter_map({'foo': 'bar'})
        {foo: "bar"}
    """
    return "{%s}" % (
        ', '.join("%s: %s" % (key, json.dumps(value))
        for key, value in data.items()
    ))


def get_match_clause(obj, name, type_registry):
    """Return node lookup by index for a match clause using unique attributes

    Args:
        obj: An object to create an index lookup.
        name: The name of the object in the query.
    Returns:
        A string with an index lookup for a cypher MATCH clause
    """

    if isinstance(obj, PersistableType):
        value = get_type_id(obj)
        return '({name}:PersistableType {{id: {value}}})'.format(
            name=name,
            value=json.dumps(object_to_db_value(value)),
        )

    if isinstance(obj, Relationship):
        if obj.start is None or obj.end is None:
            raise NoUniqueAttributeError(
                "{} is missing a start or end node".format(obj)
            )
        rel_type = get_relationship_id(type(obj))
        start_name = '{}__start'.format(name)
        end_name = '{}__end'.format(name)
        query = """
            {start_clause},
            {end_clause},
            ({start_name})-[{name}:{rel_type}]->({end_name})
        """.format(
            name=name,
            start_clause=get_match_clause(obj.start, start_name, type_registry),
            end_clause=get_match_clause(obj.end, end_name, type_registry),
            start_name=start_name,
            end_name=end_name,
            rel_type=rel_type,
        )
        return dedent(query)

    match_params = {}
    label_classes = set()
    for cls, attr_name in type_registry.get_unique_attrs(type(obj)):
        value = getattr(obj, attr_name)
        if value is not None:
            label_classes.add(cls)
            match_params[attr_name] = value
    if not match_params:
        raise NoUniqueAttributeError(
            "{} doesn't have any unique attributes".format(obj)
        )
    match_params_string = inline_parameter_map(
        dict_to_db_values_dict(match_params)
    )
    labels = ':'.join(get_type_id(cls) for cls in label_classes)

    return '({name}:{labels} {match_params_string})'.format(
        name=name,
        labels=labels,
        attr_name=attr_name,
        match_params_string=match_params_string,
    )


def get_create_types_query(cls, type_system_id, type_registry):
    """ Returns a CREATE UNIQUE query for an entire type hierarchy.

    Includes statements that create each type's attributes.

    Args:
        cls: An object to create a type hierarchy for.

    Returns:
        A tuple containing:
        (cypher query, classes to create nodes for, the object names).
    """
    hierarchy_lines = []
    set_lines = []
    classes = {}

    query_args = {
        'type_system_id': type_system_id,
        'Defines_props': type_registry.object_to_dict(Defines()),
        'InstanceOf_props': type_registry.object_to_dict(InstanceOf()),
        'DeclaredOn_props': type_registry.object_to_dict(DeclaredOn()),
    }

    # filter type relationships that we want to persist
    type_relationships = []
    for cls1, rel_cls_idx, cls2 in get_type_relationships(cls):
        if issubclass(cls2, AttributedBase):
            type_relationships.append((cls1, rel_cls_idx, cls2))

    # process type relationships
    is_first = True
    isa_props_counter = 0

    for cls1, (rel_cls, base_idx), cls2 in type_relationships:

        name1 = cls1.__name__
        type1 = type(cls1).__name__

        if name1 in classes:
            abstr1 = '(`%s`:%s)' % (name1, type1)
        else:
            abstr1 = '(`%s`:%s {%s_id_props})' % (name1, type1, name1)

        classes[name1] = cls1

        if is_first:
            is_first = False
            ln = 'ts -[:DEFINES {Defines_props}]-> %s' % abstr1
        else:
            name2 = cls2.__name__
            classes[name2] = cls2

            rel_name = get_type_id(rel_cls)
            rel_type = rel_name.upper()

            prop_name = "%s_props" % rel_name

            if rel_cls is IsA:
                prop_name = '%s_%d' % (rel_name, isa_props_counter)
                isa_props_counter += 1

                props = type_registry.object_to_dict(IsA(base_index=base_idx))
                query_args[prop_name] = props

            ln = '%s -[%s:%s]-> `%s`' % (abstr1, prop_name, rel_type, name2)
            set_lines.append('SET `%s` = {%s}' % (prop_name, prop_name))
        hierarchy_lines.append(ln)

    # process attributes
    for name, cls in classes.items():

        descriptor = type_registry.get_descriptor(cls)
        attributes = descriptor.declared_attributes
        for attr_name, attr in attributes.iteritems():
            key = "%s_%s" % (name, attr_name)

            ln = '({%s}) -[:DECLAREDON {DeclaredOn_props}]-> `%s`' % (
                key, name)
            hierarchy_lines.append(ln)

            attr_dict = type_registry.object_to_dict(
                attr, for_db=True)

            attr_dict['name'] = attr_name
            query_args[key] = attr_dict

    # processing class attributes
    for key, cls in classes.iteritems():
        # all attributes of the class to be set via the query
        cls_props = type_registry.object_to_dict(cls, for_db=True)
        query_args['%s_props' % key] = cls_props
        set_lines.append('SET `%s` = {%s_props}' % (key, key))

        # attributes which uniquely identify the class itself
        # these are used in the CREATE UNIQUE part of the query
        cls_id_props = {
            '__type__': cls_props['__type__'],
            'id': cls_props['id']
        }
        query_args['%s_id_props' % key] = cls_id_props

    quoted_names = ('`{}`'.format(cls) for cls in classes.keys())
    query = join_lines(
        'MATCH (ts:TypeSystem) WHERE ts.id = {type_system_id}'
        'CREATE UNIQUE',
        (hierarchy_lines, ','),
        (set_lines, ''),
        'RETURN %s' % ', '.join(quoted_names)
    )

    return query, classes.values(), query_args


def get_create_relationship_query(rel, type_registry):
    rel_props = type_registry.object_to_dict(rel, for_db=True)
    query = 'MATCH %s, %s CREATE n1 -[r:%s {props}]-> n2 RETURN r'

    query = query % (
        get_match_clause(rel.start, 'n1', type_registry),
        get_match_clause(rel.end, 'n2', type_registry),
        rel_props['__type__'].upper(),
    )

    return query
