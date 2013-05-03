from logging import getLogger
import uuid

from py2neo import cypher, neo4j

from kaiso.attributes import Outgoing, Incoming, String, Uuid
from kaiso.connection import get_connection
from kaiso.exceptions import UniqueConstraintError, UnknownType
from kaiso.queries import (
    get_create_types_query, get_create_relationship_query,
    get_start_clause, join_lines)
from kaiso.references import set_store_for_object
from kaiso.relationships import InstanceOf
from kaiso.serialize import (
    dict_to_db_values_dict, dict_to_object, object_to_dict, get_changes)
from kaiso.types import (
    Descriptor, Persistable, PersistableMeta, Relationship,
    AttributedBase, get_index_entries, get_index_name, is_indexable)

log = getLogger(__name__)


class TypeSystem(AttributedBase):
    """ ``TypeSystem`` is a node that represents the root
    of the type hierarchy.

    The current version of the hierarchy is tracked using
    its version attribute.
    """
    id = String(unique=True)
    version = Uuid()


def get_attr_filter(obj):
    """ Generates a dictionary, that will identify the given ``obj``
    based on it's unique attributes.

    Args:
        obj: An object to look up by index

    Returns:
        A dictionary of key-value pairs to indentify an object by.
    """
    indexes = get_index_entries(obj)
    index_filter = dict((key, value) for _, key, value in indexes)
    return index_filter


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
        self.type_system = TypeSystem(id='TypeSystem')
        self._dynamic_meta = None

    def _execute(self, query, **params):
        """ Runs a cypher query returning only raw rows of data.

        Args:
            query: A parameterized cypher query.
            params: The parameters used by the query.

        Returns:
            A generator with the raw rows returned by the connection.
        """
        log.debug('running query:\n%s', query.format(**params))

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
            obj = dict_to_object(properties, self._dynamic_meta)

            if isinstance(value, neo4j.Relationship):
                obj.start = self._convert_value(value.start_node)
                obj.end = self._convert_value(value.end_node)
            else:
                set_store_for_object(obj, self)
            return obj
        return value

    def _convert_row(self, row):
        for value in row:
            if isinstance(value, list):
                yield [self._convert_value(v) for v in value]
            else:
                yield self._convert_value(value)

    def _index_object(self, obj, node_or_rel):
        indexes = get_index_entries(obj)
        for index_name, key, value in indexes:
            if isinstance(obj, Relationship):
                index_type = neo4j.Relationship
            else:
                index_type = neo4j.Node

            log.debug(
                'indexing %s for %s using index %s',
                obj, node_or_rel, index_name)

            index = self._conn.get_or_create_index(index_type, index_name)
            index.add(key, value, node_or_rel)

        if not isinstance(obj, Relationship):
            set_store_for_object(obj, self)

    def _load_types(self):
        dyn_type = self._dynamic_meta

        for type_id, bases, attrs in self.get_type_hierarchy():
            try:
                cls = dyn_type.get_class_by_id(type_id)

                if type(cls) is not dyn_type:
                    cls = None
            except UnknownType:
                cls = None

            if cls is None:
                bases = tuple(dyn_type.get_class_by_id(base) for base in bases)
                attrs = dict((attr.name, attr) for attr in attrs)
                self._dynamic_meta(str(type_id), bases, attrs)

    def _get_changes(self, persistable):
        changes = {}
        existing = None
        obj_type = type(persistable)

        if isinstance(persistable, PersistableMeta):
            # this is a class, we need to get it and it's attrs
            idx_name = obj_type.index_name
            self._conn.get_or_create_index(neo4j.Node, idx_name)

            descr = obj_type.get_descriptor(persistable)
            query_args = {
                'type_id': descr.type_id
            }

            query = join_lines(
                'START cls=node:%s(id={type_id})' % idx_name,
                'MATCH attr -[:DECLAREDON*0..]-> cls',
                'RETURN cls, collect(attr.name?)'
            )

            rows = self.query(query, **query_args)
            existing, attrs = next(rows, (None, None))

            if existing is None:
                # have not found the cls
                return None, {}

            attrs = set(attrs)

            modified_attrs = {}

            for name, attr in descr.declared_attributes.items():
                if name not in attrs:
                    modified_attrs[name] = attr

            del_attrs = set(attrs)

            for name in Descriptor(persistable).attributes.keys():
                del_attrs.discard(name)

            for name in del_attrs:
                modified_attrs[name] = None

            if modified_attrs:
                changes['attributes'] = modified_attrs
        else:
            existing = self.get(obj_type, **get_attr_filter(persistable))

            if existing is not None:
                existing_props = object_to_dict(existing, self._dynamic_meta)
                props = object_to_dict(persistable, self._dynamic_meta)

                if isinstance(persistable, Relationship):
                    ex_start = get_attr_filter(existing.start)
                    ex_end = get_attr_filter(existing.end)
                    pers_start = get_attr_filter(persistable.start)
                    pers_end = get_attr_filter(persistable.end)

                    if ex_start != pers_start:
                        props['start'] = persistable.start

                    if ex_end != pers_end:
                        props['end'] = persistable.end

                if existing_props == props:
                    return existing, {}

                changes = get_changes(old=existing_props, new=props)

        return existing, changes

    def _update_types(self, cls):
        query, objects, query_args = get_create_types_query(
            cls, self.type_system, self._dynamic_meta)

        nodes_or_rels = next(self._execute(query, **query_args))

        for obj in objects:
            if is_indexable(obj):
                index_name = get_index_name(obj)
                self._conn.get_or_create_index(neo4j.Node, index_name)

        for obj, node_or_rel in zip(objects, nodes_or_rels):
            self._index_object(obj, node_or_rel)

        return cls

    def _update(self, persistable, existing, changes):
        for _, index_attr, _ in get_index_entries(existing):
            if index_attr in changes:
                raise NotImplementedError(
                    "We currently don't support changing unique attributes")

        set_clauses = ', '.join([
            'n.%s={%s}' % (key, key) for key, value in changes.items()
            if not isinstance(value, dict)
        ])

        if set_clauses:
            set_clauses = 'SET %s' % set_clauses
        else:
            set_clauses = ''

        if isinstance(persistable, type):
            descr = self._dynamic_meta.get_descriptor(persistable)

            query_args = {'type_id': descr.type_id}

            where = []

            for attr_name in descr.declared_attributes.keys():
                where.append('attr.name = {attr_%s}' % attr_name)
                query_args['attr_%s' % attr_name] = attr_name

            if where:
                where = ' OR '.join(where)
                where = 'WHERE not(%s)' % where
            else:
                where = ''

            index_name = self._dynamic_meta.index_name

            query = join_lines(
                'START n=node:%s(id={type_id})' % index_name,
                set_clauses,
                'MATCH attr -[r:DECLAREDON]-> n',
                where,
                'DELETE attr, r',
                'RETURN n',
            )

            self._update_types(persistable)
        else:
            start_clause = get_start_clause(existing, 'n')
            query = None

            if isinstance(persistable, Relationship):
                old_start = existing.start
                old_end = existing.end

                new_start = changes.pop('start', old_start)
                new_end = changes.pop('end', old_end)

                if old_start != new_start or old_end != new_end:
                    start_clause = '%s, %s, %s, %s, %s' % (
                        start_clause,
                        get_start_clause(old_start, 'old_start'),
                        get_start_clause(old_end, 'old_end'),
                        get_start_clause(new_start, 'new_start'),
                        get_start_clause(new_end, 'new_end')
                    )

                    rel_props = object_to_dict(persistable, self._dynamic_meta)

                    query = join_lines(
                        'START %s' % start_clause,
                        'DELETE n',
                        'CREATE new_start -[r:%s {rel_props}]-> new_end' % (
                            rel_props['__type__'].upper()
                        ),
                        'RETURN r'
                    )
                    query_args = {'rel_props': rel_props}

            if query is None:
                query = join_lines(
                    'START %s' % start_clause,
                    set_clauses,
                    'RETURN n'
                )
                query_args = changes

        try:
            (result,) = next(self._execute(query, **query_args))
        except StopIteration:
            # this can happen, if no attributes where changed on a type
            result = persistable

        if isinstance(persistable, Relationship):
            self._index_object(persistable, result)
        return result

    def _add(self, obj):
        """ Adds an object to the data store.

        It will automatically generate the type relationships
        for the the object as required and store the object itself.
        """

        query_args = {}

        if isinstance(obj, PersistableMeta):
            # object is a type; create the type and its hierarchy
            return self._update_types(obj)

        elif obj is self.type_system:
            query = 'CREATE (n {props}) RETURN n'

        elif isinstance(obj, Relationship):
            # object is a relationship
            obj_type = type(obj)

            query = get_create_relationship_query(obj, self._dynamic_meta)

        else:
            # object is an instance; create its type, its hierarchy and then
            # create the instance
            obj_type = type(obj)
            obj_type_meta = type(obj_type)

            self._update_types(obj_type)

            idx_name = obj_type_meta.index_name
            query = (
                'START cls=node:%s(id={type_id}) '
                'CREATE (n {props}) -[:INSTANCEOF {rel_props}]-> cls '
                'RETURN n'
            ) % idx_name

            query_args = {
                'type_id': obj_type.get_descriptor(obj_type).type_id,
                'rel_props': object_to_dict(
                    InstanceOf(None, None), self._dynamic_meta),
            }

        query_args['props'] = object_to_dict(obj, self._dynamic_meta, False)

        (node_or_rel,) = next(self._execute(query, **query_args))

        self._index_object(obj, node_or_rel)

        return obj

    def get_type_hierarchy(self):
        """ Returns the entire type hierarchy defined in the database.

        Returns: A generator yielding tuples of the form
        ``(type_id, bases, attrs)`` where
            - ``type_id`` identifies the type
            - ``bases`` lists the type_ids of the type's bases
            - ``attrs`` lists the attributes defined on the type
        """
        query = join_lines(
            'START %s' % get_start_clause(self.type_system, 'ts'),
            'MATCH',
            '  p=(ts -[:DEFINES]-> () <-[:ISA*0..]- tpe),',
            '  tpe <-[:DECLAREDON*0..]- attr,',
            '  tpe -[:ISA*0..1]-> base',
            'RETURN tpe.id,  length(p) AS level,',
            '  filter(bse_id in collect(distinct base.id): bse_id <> tpe.id),',
            '  filter(attr in collect(distinct attr): attr.id? <> tpe.id)',
            'ORDER BY level'
        )

        rows = self.query(query)
        return ((type_id, bases, attrs) for type_id, _, bases, attrs in rows)

    def serialize(self, obj):
        """ Serialize ``obj`` to a dictionary.

        Args:
            obj: An object to serialize

        Returns:
            A dictionary describing the object
        """
        return object_to_dict(obj, self._dynamic_meta)

    def deserialize(self, object_dict):
        """ Deserialize ``object_dict`` to an object.

        Args:
            object_dict: A serialized object dictionary

        Returns:
            An object deserialized using the type registry
        """
        return dict_to_object(object_dict, self._dynamic_meta)

    def create_type(self, name, bases, attrs):
        """ Creates a new class given the name, bases and attrs given.
        """
        return self._dynamic_meta(name, bases, attrs)

    def save(self, persistable):
        """ Stores the given ``persistable`` in the graph database.
        If a matching object (by unique keys) already exists, it will
        update it with the modified attributes.
        """
        if not isinstance(persistable, Persistable):
            raise TypeError('cannot persist %s' % persistable)

        existing, changes = self._get_changes(persistable)

        if existing is None:
            self._add(persistable)
            return persistable
        elif not changes:
            return persistable
        else:
            return self._update(persistable, existing, changes)

    def get(self, cls, **attr_filter):
        attr_filter = dict_to_db_values_dict(attr_filter)

        if not attr_filter:
            return None

        query_args = {}

        indexes = attr_filter.items()

        if issubclass(cls, (Relationship, PersistableMeta)):
            idx_name = get_index_name(cls)
            idx_key, idx_value = indexes[0]

            if issubclass(cls, Relationship):
                self._conn.get_or_create_index(neo4j.Relationship, idx_name)
                start_func = 'relationship'
            else:
                self._conn.get_or_create_index(neo4j.Node, idx_name)
                start_func = 'node'

            query = 'START nr = %s:%s(%s={idx_value}) RETURN nr' % (
                start_func, idx_name, idx_key)

            query_args['idx_value'] = idx_value
        else:
            idx_where = []
            for key, value in indexes:
                idx_where.append('n.%s! = {%s}' % (key, key))
                query_args[key] = value
            idx_where = ' or '.join(idx_where)

            idx_name = get_index_name(TypeSystem)
            query = join_lines(
                'START root=node:%s(id={idx_value})' % idx_name,
                'MATCH n -[:INSTANCEOF]-> () -[:ISA*]-> () <-[:DEFINES]- root',
                'WHERE %s' % idx_where,
                'RETURN n',
            )

            query_args['idx_value'] = self.type_system.id

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
            rel_query = 'n -[relation:{}]-> related'
        elif ref_cls is Incoming:
            rel_query = 'n <-[relation:{}]- related'

        # TODO: should get the rel name from descriptor?
        rel_query = rel_query.format(rel_cls.__name__.upper())

        query = 'START {idx_lookup} MATCH {rel_query} RETURN related, relation'

        query = query.format(
            idx_lookup=get_start_clause(obj, 'n'),
            rel_query=rel_query
        )

        return self.query(query)

    def delete(self, obj):
        """ Deletes an object from the store.

        Args:
            obj: The object to delete.

        Returns:
            A tuple: with (number of nodes removed, number of rels removed)
        """
        if isinstance(obj, Relationship):
            query = join_lines(
                'START {}, {}',
                'MATCH n1 -[rel]-> n2',
                'DELETE rel',
                'RETURN 0, count(rel)'
            ).format(
                get_start_clause(obj.start, 'n1'),
                get_start_clause(obj.end, 'n2'),
            )
        elif isinstance(obj, PersistableMeta):
            query = join_lines(
                'START {}',
                'MATCH attr -[:DECLAREDON]-> obj',
                'DELETE attr',
                'MATCH obj -[rel]- ()',
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_start_clause(obj, 'obj')
            )
        else:
            query = join_lines(
                'START {}',
                'MATCH obj -[rel]- ()',
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_start_clause(obj, 'obj')
            )

        # TODO: delete node/rel from indexes

        return next(self._execute(query))

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

    def initialize(self):
        type_name = 'Storage_PersistableType_{}'.format(uuid.uuid4())
        self._dynamic_meta = type(type_name, (PersistableMeta,), {})

        idx_name = get_index_name(TypeSystem)
        self._conn.get_or_create_index(neo4j.Node, idx_name)
        self.save(self.type_system)

        self._load_types()
