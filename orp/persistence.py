from functools import wraps
import logging

from py2neo import cypher, neo4j

from orp.connection import get_connection
from orp.descriptors import (
    get_descriptor, get_named_descriptor, get_indexes)
from orp.exceptions import NoIndexesError, UniqueConstraintError
from orp.query import encode_query_values
from orp.types import PersistableType, Persistable
from orp.relationships import Relationship, InstanceOf, IsA


_log = logging.getLogger(__name__)

def first(items):
    ''' Returns the first item of an iterable object.

    Args:
        items: An iterable object

    Returns:
        The first item from items.
    '''
    return iter(items).next()


def unique(fn):
    ''' Wraps a function to return only unique items.
    The wrapped function must return an iterable object.
    When the wrapped function is called, each item from the iterable
    will be yielded only once and duplicates will be ignored.

    Args:
        fn: The function to be wrapped.

    Returns:
        A wrapper function for fn.
    '''
    @wraps(fn)
    def wrapped(*args, **kwargs):
        items = set()
        for item in fn(*args, **kwargs):
            if item not in items:
                items.add(item)
                yield item
    return wrapped


def object_to_dict(obj):
    ''' Converts a persistable object to a dict.

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
    '''
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
    ''' Converts a dict into a persistable object.

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
    '''

    type_name = properties['__type__']
    descriptor = get_named_descriptor(type_name)

    cls = descriptor.cls

    if issubclass(cls, type):
        obj = get_named_descriptor(properties['name']).cls
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


@unique
def get_type_relationships(obj):
    ''' Generates a list of the type relationships of an object.
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
    '''
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


def get_index_query(obj, name=None):
    ''' Returns a node lookup by index as used by the START clause.

    Args:
        obj: An object to create a index lookup.
        name: The name of the object in the query.
                If name is None obj.__name__ will be used.
    Returns:
        A string with index lookup of a cypher START clause.
    '''

    if name is None:
        name = obj.__name__

    index_name, key, value = first(get_indexes(obj))

    query = '%s = node:%s(%s="%s")' % (name, index_name, key, value)
    return query


def get_create_types_query(obj):
    ''' Returns a CREATE UNIQUE query for an entire type hierarchy.

    Args:
        obj: An object to create a type hierarchy for.

    Returns:
        A tuple containing:
        (cypher query, objects to create nodes for, the object names).
    '''

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
       value in the None dictionary"""
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
    ''' Provides a queryable object store.

    The object store can store any object as long as it's type is registered.
    This includes instances of Persistable, PersistableType
    and subclasses of either.

    InstanceOf and IsA relationships are automatically generated,
    when persisting an object.
    '''
    def __init__(self, connection_uri):
        ''' Initializes a Storage object.

        Args:
            connection_uri: A URI used to connect to the graph database.
        '''
        self._conn = get_connection(connection_uri)

    def _execute(self, query, **params):
        ''' Runs a cypher query returning only raw rows of data.

        Args:
            query: A parameterized cypher query.
            params: The parameters used by the query.

        Returns:
            A generator with the raw rows returned by the connection.
        '''

        rows, _ = cypher.execute(self._conn, query, params)
        for row in rows:
            yield row

    def _convert_value(self, value):
        ''' Converts a py2neo primitive(Node, Relationship, basic object)
        to an equvalent python object.
        Any value which cannot be converted, will be returned as is.

        Args:
            value: The value to convert.

        Returns:
            The converted value.
        '''
        if isinstance(value, (neo4j.Node, neo4j.Relationship)):
            properties = value.get_properties()
            obj = dict_to_object(properties)

            if isinstance(value, neo4j.Relationship):
                obj.start = self._convert_value(value.start_node)
                obj.end = self._convert_value(value.end_node)

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
        index_filter = encode_query_values(index_filter)
        descriptor = get_descriptor(cls)

        # MJB: can we consider a different signature that avoids this assert?
        # MJB: something like:
        # MJB: def get(self, cls, (key, value)):
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
           matches any of the given objects' unique indexes. """

        found = []
        indexes = get_indexes(obj)

        if isinstance(obj, Relationship):
            start_func = 'rel'
        else:
            start_func = 'node'

        for index in indexes:
            index_name, index_key, index_val = index
            start_clause = 'x={}:{}({}="{}")'.format(
                start_func, index_name, index_key, index_val
            )

            query = '''START {}
                       RETURN x'''.format(start_clause)
            try:
                result = self._execute(query, node_props=object_to_dict(obj))
                result = list(result)
            except cypher.CypherError as exc:
                if exc.exception != 'MissingIndexException':
                    raise
                # MissingIndexException is ok, treat as no result
            else:
                if result:
                    found.append(first(first(result)))

        return found

    def _make_create_relationship_query(self, rel, unique=False):
        rel_props = object_to_dict(rel)
        maybe_unique =  'UNIQUE' if unique else ''
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
           given one"""
        props = object_to_dict(persistable)
        indexes = get_indexes(persistable)
        has_indexes = bool(next(indexes, False))

        if isinstance(persistable, Relationship):
            if has_indexes:
                raise NotImplementedError('Relationships are not yet '
                    'being indexed')

            query = self._make_create_relationship_query(persistable,
                unique=True)
            _log.warning(query)
            print query
            result = self._execute(query, rel_props=props)
            result = list(result)
            import pytest; pytest.set_trace()

        elif isinstance(persistable, Persistable):
            if not has_indexes:
                raise NoIndexesError("Can't replace an object with no indexes")

            existing = self._get_by_unique(persistable)

            if existing:
                # all the nodes returned should be the same
                for node in existing:
                    if node.id != existing[0].id:
                        raise UniqueConstraintError("Can't create {} as"
                            "existing data contain values "
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

                set_clauses = ['n.%s={%s}' % (key, key)
                    for key in changes]
                set_clause = ','.join(set_clauses)

                query = '''START %s
                           SET %s
                           RETURN n''' % (start_clause, set_clause)

                result = self._execute(query, **changes)
                return first(first(result))

            # if we get this far, there's no existing node, and
            # we should create a new one
            return self.add(persistable)

    def query(self, query, **params):
        ''' Queries the store given a parameterized cypher query.

        Args:
            query: A parameterized cypher query.
            params: query: A parameterized cypher query.

        Returns:
            A generator with tuples containing stored objects or values.
        '''
        params = encode_query_values(params)
        for row in self._execute(query, **params):
            yield tuple(self._convert_row(row))

    def add(self, obj):
        ''' Adds an object to the data store.

        It will automatically generate the type relationships
        for the the object as required and store the object itself.

        Args:
            obj: The object to store.
        '''
        if not can_add(obj):
            raise TypeError('cannot persist %s' % obj)

        existing = self._get_by_unique(obj)

        if (not obj is Persistable) and existing:
            raise UniqueConstraintError(
                'Can not add {}s as {} exist'.format(obj, existing)
            )

        if isinstance(obj, Relationship):
            query = self._make_create_relationship_query(obj)
            first(self._execute(query, rel_props=object_to_dict(obj)))
            return

        if obj is Persistable:
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

        nodes = first(self._execute(query, **query_args))

        # index all the created nodes
        # infact, we are indexing all nodes, created or not ;-(
        for node, obj in zip(nodes, objects):
            indexes = get_indexes(obj)
            for index_name, key, value in indexes:
                index = self._conn.get_or_create_index(neo4j.Node, index_name)
                index.add(key, value, node)


def can_add(obj):
    ''' Returns whether or not an object can be added to the db.

        We can add instances of Persistable or Relationship.
        In addition it is also possible to add sub-classes of
        Persistable.
    '''
    return (
        (isinstance(obj, type) and issubclass(obj, Persistable)) or
        isinstance(obj, Persistable) or
        isinstance(obj, Relationship)
    )

