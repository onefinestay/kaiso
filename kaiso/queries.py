from kaiso.exceptions import NoUniqueAttributeError
from kaiso.relationships import InstanceOf, IsA, DeclaredOn, Defines
from kaiso.serialize import dict_to_db_values_dict
from kaiso.types import (
    AttributedBase, get_index_name, Relationship, get_type_id, PersistableType)
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
    if index is None:
        raise NoUniqueAttributeError()

    if isinstance(obj, Relationship):
        index_type = "rel"
    else:
        index_type = "node"
    query = '{}={}:{}({}="{}")'.format(name, index_type, *index)
    return query


def get_match_clause(obj, name, type_registry):
    """Return params to use to lookup a node using unique attributes

    Args:
        obj: An object to create an index lookup.
        name: The name of the object in the query.
    Returns:
        A tuple (match, where), where
            match is a match MATCH clause and where is a WHERE clause, together
            finding obj, with a name `name`
    """

    if isinstance(obj, PersistableType):
        cls = PersistableType
        attr_name = 'id'
        value = get_type_id(obj)

    else:
        unique = next(type_registry.get_unique_attrs(type(obj)), None)
        if unique is None:
            raise NoUniqueAttributeError()

        cls, attr_name = unique
        value = getattr(obj, attr_name)

    type_id = get_type_id(cls)

    match = "(%s:%s)" % (name, type_id)
    param_name = '%s_%s' % (name, attr_name)
    where = "%s.%s = {%s}" % (name, attr_name, param_name)

    param = dict_to_db_values_dict({param_name: value})

    return match, where, param


def get_create_types_query(cls, root, type_registry):
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
        'root_id': root.id,
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
            ln = 'root -[:DEFINES {Defines_props}]-> %s' % abstr1
        else:
            name2 = cls2.__name__
            classes[name2] = cls2

            rel_name = rel_cls.__name__
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
        'START root=node:%s(id={root_id})' % get_index_name(type(root)),
        'CREATE UNIQUE',
        (hierarchy_lines, ','),
        (set_lines, ''),
        'RETURN %s' % ', '.join(quoted_names)
    )

    return query, classes.values(), query_args


def get_create_relationship_query(rel, type_registry):
    rel_props = type_registry.object_to_dict(rel, for_db=True)

    start_match, start_where, start_param = get_match_clause(
        rel.start, 'n1', type_registry)
    end_match, end_where, end_param = get_match_clause(
        rel.end, 'n2', type_registry)

    query = join_lines(
        "MATCH",
        join_lines(
            start_match,
            end_match,
            sep=','
        ),
        "WHERE",
        join_lines(
            start_where,
            end_where,
            sep=' AND '
        ),
        "CREATE  n1 -[r:%s {props}]-> n2 RETURN r",
    )

    query = query % (
        rel_props['__type__'].upper(),
    )

    params = start_param
    params.update(end_param)

    return query, params
