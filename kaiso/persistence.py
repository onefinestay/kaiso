from functools import wraps
import logging

from py2neo import cypher, neo4j

from kaiso.connection import get_connection
from kaiso.descriptors import (
    get_descriptor, get_descriptor_by_name, get_indexes)
from kaiso.exceptions import (NoIndexesError, MultipleObjectsFound,
    UniqueConstraintError)
from kaiso.iter_helpers import unique
from kaiso.references import set_store_for_object
from kaiso.attributes import Outgoing, Incoming
from kaiso.attributes.bases import get_attibute_for_type
from kaiso.relationships import InstanceOf, IsA
from kaiso.types import PersistableType, Persistable, Relationship


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
            '__type__': 'Persistable',
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

    type_name = properties['__type__']
    descriptor = get_descriptor_by_name(type_name)

    cls = descriptor.cls

    if issubclass(cls, type):
        obj = get_descriptor_by_name(properties['name']).cls
    else:
        obj = cls.__new__(cls)

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
        get_type_relationships(Persistable())

        (object, InstanceOf, type),
        (type, IsA, object),
        (type, InstanceOf, type),
        (PersistableType, IsA, type),
        (PersistableType, InstanceOf, type),
        (Persistable, IsA, object),
        (Persistable, InstanceOf, PersistableType),
        (<Persistable object>, InstanceOf, Persistable)

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


def get_index_queries(obj, name=None):
    """Returns a list of the possible
       node lookups by index as used by the START clause.

    Args:
        obj: An object to create a index lookup.
        name: The name of the object in the query.
                If name is None obj.__name__ will be used.
    Returns:
        A list of strings  with index lookup of a cypher START clause.
    """
    queries = []

    if name is None:
        name = obj.__name__

    if isinstance(obj, Relationship):
        start_func = 'relationship'
    else:
        start_func = 'node'

    indexes = get_indexes(obj)
    for index_name, index_key, index_val in indexes:
        queries.append('{}={}:{}({}="{}")'.format(
            name, start_func, index_name, index_key, index_val
        ))

    return queries


def get_index_query(obj, name=None):
    """ Returns a node lookup by index as used by the START clause.

    Args:
        obj: An object to create a index lookup.
        name: The name of the object in the query.
                If name is None obj.__name__ will be used.
    Returns:
        A string with index lookup of a cypher START clause.
    """
    queries = get_index_queries(obj, name)
    return queries[0] if queries else None


def get_create_types_query(obj):
    """ Returns a CREATE UNIQUE query for an entire type hierarchy.

    Args:
        obj: An object to create a type hierarchy for.

    Returns:
        A tuple containing:
        (cypher query, objects to create nodes for, the object names).
    """

    lines = []
    objects = {'Persistable': Persistable}

    for obj1, rel_cls, obj2 in get_type_relationships(obj):
        # this filters out the types, which we don't want to persist
        if issubclass(obj2, Persistable):
            if isinstance(obj1, type):
                name1 = obj1.__name__
            else:
                name1 = 'new_obj'

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
        'START %s' % get_index_query(Persistable),
        'CREATE UNIQUE')
    query += ('    ' + ',\n    '.join(lines),)
    query += ('RETURN %s' % ', '.join(keys),)
    query = '\n'.join(query)

    return query, objects, keys


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
    This includes instances of Persistable, PersistableType
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

    def _root_exists(self):
        try:
            # we have to make sure we have a starting point for
            # the type hierarchy, for now that is Persistable
            obj = self.get(PersistableType, name='Persistable')
            if obj is not Persistable:
                raise Exception("Db is broken")  # TODO: raise DbIsBroken()

            return True
        except:  # (IndexDoesn'texistYet or None returned)
            return False

    def get(self, cls, **index_filter):
        index_filter = dict_to_db_values_dict(index_filter)
        descriptor = get_descriptor(cls)

        # TODO: can we consider a different signature that avoids this assert?
        # TODO: something like:
        # TODO: def get(self, cls, (key, value)):
        assert len(index_filter) == 1, "only one index allowed at a time"
        key, value = index_filter.items()[0]

        index_name = descriptor.get_index_name_for_attribute(key)
        node = self._conn.get_indexed_node(index_name, key, value)
        if node is None:
            return None

        obj = self._convert_value(node)
        return obj

    def _get_by_unique(self, obj):
        """Return a list of any existing data from the database that
           matches any of the given objects' unique indexes.
        """
        found = []

        for clause in get_index_queries(obj, 'x'):
            query = '''START {}
                       RETURN x'''.format(clause)
            try:
                result = self._execute(query, node_props=object_to_dict(obj))
                result = list(result)
            except cypher.CypherError as exc:
                if exc.exception != 'MissingIndexException':
                    raise  # pragma: no cover
                # MissingIndexException is ok, treat as no result
            else:
                if result:
                    found.append(result[0][0])

        return found

    def _make_create_relationship_query(self, rel, unique=False):
        rel_props = object_to_dict(rel)
        maybe_unique = 'UNIQUE' if unique else ''
        query = 'START %s, %s CREATE %s n1 -[r:%s {rel_props}]-> n2 RETURN r'
        query = query % (
            get_index_query(rel.start, 'n1'),
            get_index_query(rel.end, 'n2'),
            maybe_unique,
            rel_props['__type__'].upper(),
        )

        return query

    def replace(self, persistable):
        """Store the given persistable in the graph database. If a matching
           object (by unique keys) already exists, replace it with the
           given one
        """
        props = object_to_dict(persistable)
        indexes = get_indexes(persistable)
        has_indexes = bool(next(indexes, False))

        if isinstance(persistable, Relationship):
            if has_indexes:
                raise NotImplementedError(
                    'Indexed Relationships cannot be replaced.')

            query = self._make_create_relationship_query(
                persistable, unique=True)
            result = self._execute(query, rel_props=props)
            result = list(result)

        elif isinstance(persistable, Persistable):
            if not has_indexes:
                raise NoIndexesError("Can't replace an object with no indexes")

            existing = self._get_by_unique(persistable)

            if existing:
                # all the nodes returned should be the same
                for node in existing:
                    if node.id != existing[0].id:
                        raise UniqueConstraintError(
                            "Can't create {} as existing data contain values "
                            "that must be unique: {} vs {}".format(
                                persistable, node, existing[0]))

                # we have one existing node and all the unique indexes match

                # if the rest of the properties are the same,
                # we have nothing to do
                existing_node = self._convert_value(existing[0])
                existing_props = object_to_dict(existing_node)
                if existing_props == props:
                    return existing_node

                # otherwise update with new properties

                start_clause = get_index_query(existing_node, 'n')
                changes = _get_changes(old=existing_props, new=props)

                set_clauses = [
                    'n.%s={%s}' % (key, key) for key in changes]
                set_clause = ','.join(set_clauses)

                query = '''START %s
                           SET %s
                           RETURN n''' % (start_clause, set_clause)

                result = self._execute(query, **changes)
                return next(result)[0]

            # if we get this far, there's no existing node, and
            # we should create a new one
            self.add(persistable)
            return persistable

    def get_related_objects(self, rel_cls, ref_cls, obj):

        if ref_cls is Outgoing:
            rel_query = 'n -[:{}]-> related'
        elif ref_cls is Incoming:
            rel_query = 'n <-[:{}]- related'

        # TODO: should get the rel name from descriptor?
        rel_query = rel_query.format(rel_cls.__name__.upper())

        query = 'START {idx_lookup} MATCH {rel_query} RETURN related'

        query = query.format(
            idx_lookup=get_index_query(obj, 'n'),
            rel_query=rel_query
        )

        rows = self.query(query)
        related_objects = (related_obj for (related_obj,) in rows)

        return related_objects

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

    def add(self, obj):
        """ Adds an object to the data store.

        It will automatically generate the type relationships
        for the the object as required and store the object itself.

        Args:
            obj: The object to store.
        """
        if not can_add(obj):
            raise TypeError('cannot persist %s' % obj)

        existing = self._get_by_unique(obj)

        if (not obj is Persistable) and existing:
            raise UniqueConstraintError(
                'Can not add {}s as {} exist'.format(obj, existing)
            )

        if isinstance(obj, Relationship):
            query = self._make_create_relationship_query(obj)
            query_args = {'rel_props': object_to_dict(obj)}
            objects = [obj]

        elif obj is Persistable:
            if self._root_exists():
                return
            else:
                # create the PersistableType node.
                # if we had a standard start node, we would not need this
                query = 'CREATE (n {props}) RETURN n'
                query_args = {'props': object_to_dict(Persistable)}
                objects = [Persistable]
        else:
            if not self._root_exists():
                self.add(Persistable)

            query, objects, keys = get_create_types_query(obj)

            query_args = {
                'InstanceOf_props': object_to_dict(InstanceOf(None, None)),
                'IsA_props': object_to_dict(IsA(None, None))
            }

            for key, obj in zip(keys, objects):
                query_args['%s_props' % key] = object_to_dict(obj)

        items = next(self._execute(query, **query_args))

        # index all the created nodes or relationships
        # note, that all nodes in the type-chain for the added object
        # will be re-indexed if they already existed
        for node_or_rel, obj in zip(items, objects):
            indexes = get_indexes(obj)
            for index_name, key, value in indexes:
                if isinstance(obj, Relationship):
                    index_type = neo4j.Relationship
                else:
                    index_type = neo4j.Node

                index = self._conn.get_or_create_index(index_type, index_name)

                index.add(key, value, node_or_rel)

            if not isinstance(obj, Relationship):
                set_store_for_object(obj, self)

    def delete(self, obj):
        """ Deletes an object from the store.

        Args:
            obj: The object to delete.
        """

        if isinstance(obj, Relationship):
            query = 'START {}, {} MATCH n1 -[rel]-> n2 DELETE rel'.format(
                get_index_query(obj.start, 'n1'),
                get_index_query(obj.end, 'n2'),
            )
        else:
            query = 'START {} MATCH obj -[rel]- () DELETE obj, rel'.format(
                get_index_query(obj, 'obj'))

        cypher.execute(self._conn, query)

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

        We can add instances of Persistable or Relationship.
        In addition it is also possible to add sub-classes of
        Persistable.
    """
    return (
        (isinstance(obj, type) and issubclass(obj, Persistable)) or
        isinstance(obj, Persistable) or
        isinstance(obj, Relationship)
    )
