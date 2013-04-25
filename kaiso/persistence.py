from py2neo import cypher, neo4j

from kaiso.attributes import Outgoing, Incoming
from kaiso.attributes.bases import get_attibute_for_type
from kaiso.connection import get_connection
from kaiso.exceptions import UniqueConstraintError, DeserialisationError
from kaiso.iter_helpers import unique
from kaiso.references import set_store_for_object
from kaiso.relationships import InstanceOf, IsA
from kaiso.types import (
    Persistable, PersistableMeta, Entity, Relationship, Attribute,
    get_descriptor, get_descriptor_by_name,
    get_indexes, get_index_name, is_indexable)


def object_to_dict(obj):
    """ Converts a persistable object to a dict.

    The generated dict will contain a __type__ key, for which the value will
    be the type_name as given by the descriptor for type(obj).

    If the object is a class a name key-value pair will be
    added to the generated dict, with the value being the type_name given
    by the descriptor for the object.

    For any other object all the attributes as given by the object's
    type descriptpr will be added to the dict and encoded as required.

    Args:
        obj: A persistable  object.

    Returns:
        Dictionary with attributes encoded in basic types
        and type information for deserialization.
        e.g.
        {
            '__type__': 'Entity',
            'attr1' : 1234
        }
    """
    obj_type = type(obj)

    descr = get_descriptor(obj_type)

    properties = {
        '__type__': descr.type_name,
    }

    if isinstance(obj, type):
        properties['name'] = get_descriptor(obj).type_name
    elif isinstance(obj, Attribute):
        properties['unique'] = obj.unique
    else:
        for name, attr in descr.members.items():
            value = attr.to_db(getattr(obj, name))
            if value is not None:
                properties[name] = value
    return properties


def dict_to_object(properties):
    """ Converts a dict into a persistable object.

    The properties dict needs at least a __type__ key containing the name of
    any registered class.
    The type key defines the type of the object to return.

    If the registered class for the __type__ is a meta-class,
    i.e. a subclass of <type>, a name key is assumed to be present and
    the registered class idendified by it's value is returned.

    If the registered class for the __type__ is standard class,
    i.e. an instance of <type>, and object of that class will be created
    with attributes as defined by the remaining key-value pairs.

    Args:
        properties: A dict like object.

    Returns:
        A persistable object.
    """

    try:
        type_name = properties['__type__']
    except KeyError:
        raise DeserialisationError(
            'properties "{}" missing __type__ key'.format(properties))

    descriptor = get_descriptor_by_name(type_name)

    cls = descriptor.cls

    if issubclass(cls, type):
        obj = get_descriptor_by_name(properties['name']).cls
    else:
        obj = cls.__new__(cls)

        if issubclass(cls, Attribute):
            for attr_name, value in properties.iteritems():
                setattr(obj, attr_name, value)
        else:
            for attr_name, attr in descriptor.members.items():
                try:
                    value = properties[attr_name]
                except KeyError:
                    value = attr.default
                else:
                    value = attr.to_python(value)

                setattr(obj, attr_name, value)

    return obj


def object_to_db_value(obj):
    try:
        attr_cls = get_attibute_for_type(type(obj))
    except KeyError:
        return obj
    else:
        return attr_cls.to_db(obj)


def dict_to_db_values_dict(data):
    return {k: object_to_db_value(v) for k, v in data.items()}


@unique
def get_type_relationships(obj):
    """ Generates a list of the type relationships of an object.
    e.g.
        get_type_relationships(Entity())

        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableMeta, IsA, type),
        (PersistableMeta, InstanceOf, type),
        (Entity, IsA, object),
        (Entity, InstanceOf, PersistableMeta),
        (<Entity object>, InstanceOf, Entity)

    Args:
        obj:    An object to generate the type relationships for.

    Returns:
        A generator, generating tuples
            (object, relatsionship type, related obj)
    """
    obj_type = type(obj)

    if obj_type is not type:
        for item in get_type_relationships(obj_type):
            yield item

    if isinstance(obj, type):
        for base in obj.__bases__:
            for item in get_type_relationships(base):
                yield item
            yield obj, IsA, base

    yield obj, InstanceOf, obj_type


def get_index_filter(obj):
    indexes = get_indexes(obj)
    index_filter = {key: value for _, key, value in indexes}
    return index_filter


def get_start_clause(obj, name):
    """ Returns a node lookup by index as used by the START clause.

    Args:
        obj: An object to create an index lookup.
        name: The name of the object in the query.
    Returns:
        A string with index lookup of a cypher START clause.
    """

    index = next(get_indexes(obj), None)
    query = '{}=node:{}({}="{}")'.format(name, *index)
    return query


def get_create_types_query(obj):
    """ Returns a CREATE UNIQUE query for an entire type hierarchy.

    Args:
        obj: An object to create a type hierarchy for.

    Returns:
        A tuple containing:
        (cypher query, objects to create nodes for, the object names).
    """

    lines = []
    objects = {'Entity': Entity}

    for obj1, rel_cls, obj2 in get_type_relationships(obj):
        # this filters out the types, which we don't want to persist
        if issubclass(obj2, Entity):
            name1 = obj1.__name__

            if name1 in objects:
                abstr1 = name1
            else:
                abstr1 = '(%s {%s_props})' % (name1, name1)

            name2 = obj2.__name__

            objects[name1] = obj1
            objects[name2] = obj2

            rel_name = rel_cls.__name__
            rel_type = rel_name.upper()

            ln = '%s -[:%s {%s_props}]-> %s' % (
                abstr1, rel_type, rel_name, name2)

            lines.append(ln)

    keys, objects = zip(*objects.items())

    query = (
        'START Entity=node:%s(name="Entity")' % get_index_name(type(Entity)),
        'CREATE UNIQUE'
    )
    query += ('    ' + ',\n    '.join(lines),)
    query += ('RETURN %s' % ', '.join(keys),)
    query = '\n'.join(query)

    return query, objects, keys


def get_create_relationship_query(rel):
    rel_props = object_to_dict(rel)
    query = 'START %s, %s CREATE n1 -[r:%s {rel_props}]-> n2 RETURN r'

    query = query % (
        get_start_clause(rel.start, 'n1'),
        get_start_clause(rel.end, 'n2'),
        rel_props['__type__'].upper(),
    )

    return query


def _get_changes(old, new):
    """Return a changes dictionary containing the key/values in new that are
       different from old. Any key in old that is not in new will have a None
       value in the resulting dictionary
    """
    changes = {}

    # check for any keys that have changed, put their new value in
    for key, value in new.items():
        if old.get(key) != value:
            changes[key] = value

    # if a key has dissappeared in new, put a None in changes, which
    # will remove it in neo
    for key in old.keys():
        if key not in new:
            changes[key] = None

    return changes


class Storage(object):
    """ Provides a queryable object store.

    The object store can store any object as long as it's type is registered.
    This includes instances of Entity, PersistableMeta
    and subclasses of either.

    InstanceOf and IsA relationships are automatically generated,
    when persisting an object.
    """
    def __init__(self, connection_uri):
        """ Initializes a Storage object.

        Args:
            connection_uri: A URI used to connect to the graph database.
        """
        self._conn = get_connection(connection_uri)

    def _execute(self, query, **params):
        """ Runs a cypher query returning only raw rows of data.

        Args:
            query: A parameterized cypher query.
            params: The parameters used by the query.

        Returns:
            A generator with the raw rows returned by the connection.
        """

        rows, _ = cypher.execute(self._conn, query, params)
        for row in rows:
            yield row

    def _convert_value(self, value):
        """ Converts a py2neo primitive(Node, Relationship, basic object)
        to an equvalent python object.
        Any value which cannot be converted, will be returned as is.

        Args:
            value: The value to convert.

        Returns:
            The converted value.
        """
        if isinstance(value, (neo4j.Node, neo4j.Relationship)):
            properties = value.get_properties()
            obj = dict_to_object(properties)

            if isinstance(value, neo4j.Relationship):
                obj.start = self._convert_value(value.start_node)
                obj.end = self._convert_value(value.end_node)
            else:
                set_store_for_object(obj, self)
            return obj
        return value

    def _convert_row(self, row):
        for value in row:
            yield self._convert_value(value)

    def _ensure_type_index_exists(self):
        """ Creates PersistableMeta node index if it doesn't already exist.
        """
        self._conn.get_or_create_index(
            neo4j.Node, get_index_name(PersistableMeta))

    def _index_object(self, obj, node_or_rel):
        indexes = get_indexes(obj)
        for index_name, key, value in indexes:
            if isinstance(obj, Relationship):
                index_type = neo4j.Relationship
                index = self._conn.get_or_create_index(index_type, index_name)
            else:
                index_type = neo4j.Node
                index = self._conn.get_index(index_type, index_name)

            index.add(key, value, node_or_rel)

        if not isinstance(obj, Relationship):
            set_store_for_object(obj, self)

    def _add_types(self, cls):
        self._ensure_type_index_exists()

        if cls is Entity:
            query = 'CREATE (n {props}) RETURN n'
            query_args = {'props': object_to_dict(Entity)}
            objects = [cls]
        else:
            # we have to make sure our root of the types exists
            self.save(Entity)

            query, objects, keys = get_create_types_query(cls)

            query_args = {
                'IsA_props': object_to_dict(IsA(None, None))
            }
            for key, obj in zip(keys, objects):
                query_args['%s_props' % key] = object_to_dict(obj)

        nodes_or_rels = next(self._execute(query, **query_args))

        for obj in objects:
            if is_indexable(obj):
                index_name = get_index_name(obj)
                self._conn.get_or_create_index(neo4j.Node, index_name)

        for obj, node_or_rel in zip(objects, nodes_or_rels):
            self._index_object(obj, node_or_rel)

        return cls

    def _add(self, obj):
        """ Adds an object to the data store.

        It will automatically generate the type relationships
        for the the object as required and store the object itself.
        """

        if isinstance(obj, Relationship):
            query = get_create_relationship_query(obj)
            query_args = {'rel_props': object_to_dict(obj)}

        elif isinstance(obj, PersistableMeta):
            return self._add_types(obj)
        else:
            obj_type = type(obj)
            self._add_types(obj_type)

            idx_name = get_index_name(type(obj_type))
            query = (
                'START cls=node:%s(name={type_name}) '
                'CREATE (n {node_props}) -[:INSTANCEOF {rel_props}]-> cls '
                'RETURN n'
            ) % idx_name

            query_args = {
                'type_name': obj_type.__name__,
                'node_props': object_to_dict(obj),
                'rel_props': object_to_dict(InstanceOf(None, None)),
            }

        (node_or_rel,) = next(self._execute(query, **query_args))

        self._index_object(obj, node_or_rel)

        return obj

    def save(self, persistable):
        """ Stores the given ``persistable`` in the graph database.
        If a matching object (by unique keys) already exists, it will
        update it with the modified attributes.
        """
        if not can_add(persistable):
            raise TypeError('cannot persist %s' % persistable)

        self._ensure_type_index_exists()

        existing = self.get(type(persistable), **get_index_filter(persistable))

        if existing is None:
            return self._add(persistable)

        existing_props = object_to_dict(existing)
        props = object_to_dict(persistable)

        if existing_props == props:
            # no changes
            return existing

        changes = _get_changes(old=existing_props, new=props)
        for (_, index_attr, _) in get_indexes(existing):
            if index_attr in changes:
                raise NotImplementedError(
                    "We currently don't support changing unique attributes")

        start_clause = get_start_clause(existing, 'n')
        set_clauses = [
            'n.%s={%s}' % (key, key) for key in changes]
        set_clause = ','.join(set_clauses)

        query = '''START %s
                   SET %s
                   RETURN n''' % (start_clause, set_clause)

        result = self._execute(query, **changes)
        return next(result)[0]

    def get(self, cls, **index_filter):
        index_filter = dict_to_db_values_dict(index_filter)

        query_args = {}

        indexes = index_filter.items()
        if len(indexes) == 0:
            return None

        if issubclass(cls, (Relationship, PersistableMeta)):
            idx_name = get_index_name(cls)
            idx_key, idx_value = indexes[0]

            if issubclass(cls, Relationship):
                self._conn.get_or_create_index(neo4j.Relationship, idx_name)
                start_func = 'relationship'
            else:
                start_func = 'node'

            query = 'START nr = %s:%s(%s={idx_value}) RETURN nr' % (
                start_func, idx_name, idx_key)

            query_args['idx_value'] = idx_value
        else:
            idx_where = []
            for key, value in indexes:
                idx_where.append('n.%s? = {%s}' % (key, key))
                query_args[key] = value

            idx_where = ' or '.join(idx_where)

            idx_name = get_index_name(PersistableMeta)
            query = (
                'START tpe=node:%s(name={idx_value}) '
                'MATCH n -[:INSTANCEOF]-> () -[:ISA*]-> tpe '
                'WHERE %s '
                'RETURN n'
            ) % (idx_name, idx_where)
            query_args['idx_value'] = 'Entity'

        found = [node for (node,) in self._execute(query, **query_args)]

        if not found:
            return None

        # all the nodes returned should be the same
        first = found[0]
        for node in found:
            if node.id != first.id:
                raise UniqueConstraintError((
                    "Multiple nodes ({}) found for unique lookup for "
                    "{}").format(found, cls))

        obj = self._convert_value(first)
        return obj

    def get_related_objects(self, rel_cls, ref_cls, obj):

        if ref_cls is Outgoing:
            rel_query = 'n -[:{}]-> related'
        elif ref_cls is Incoming:
            rel_query = 'n <-[:{}]- related'

        # TODO: should get the rel name from descriptor?
        rel_query = rel_query.format(rel_cls.__name__.upper())

        query = 'START {idx_lookup} MATCH {rel_query} RETURN related'

        query = query.format(
            idx_lookup=get_start_clause(obj, 'n'),
            rel_query=rel_query
        )

        rows = self.query(query)
        related_objects = (related_obj for (related_obj,) in rows)

        return related_objects

    def delete(self, obj):
        """ Deletes an object from the store.

        Args:
            obj: The object to delete.
        """

        if isinstance(obj, Relationship):
            query = 'START {}, {} MATCH n1 -[rel]-> n2 DELETE rel'.format(
                get_start_clause(obj.start, 'n1'),
                get_start_clause(obj.end, 'n2'),
            )
        else:
            query = 'START {} MATCH obj -[rel]- () DELETE obj, rel'.format(
                get_start_clause(obj, 'obj'))

        # TODO: delete node/rel from indexes

        cypher.execute(self._conn, query)

    def query(self, query, **params):
        """ Queries the store given a parameterized cypher query.

        Args:
            query: A parameterized cypher query.
            params: query: A parameterized cypher query.

        Returns:
            A generator with tuples containing stored objects or values.
        """
        params = dict_to_db_values_dict(params)
        for row in self._execute(query, **params):
            yield tuple(self._convert_row(row))

    def delete_all_data(self):
        """ Removes all nodes, relationships and indexes in the store.

            WARNING: This will destroy everything in your Neo4j database.
        """
        self._conn.clear()
        for index_name in self._conn.get_indexes(neo4j.Node).keys():
            self._conn.delete_index(neo4j.Node, index_name)
        for index_name in self._conn.get_indexes(neo4j.Relationship).keys():
            self._conn.delete_index(neo4j.Relationship, index_name)


def can_add(obj):
    """ Returns True if obj can be added to the db.

        We can add instances of Entity or Relationship.
        In addition it is also possible to add sub-classes of
        Entity.
    """
    return isinstance(obj, Persistable)
