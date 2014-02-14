from logging import getLogger
import uuid

from py2neo import cypher, neo4j

from kaiso.attributes import Outgoing, Incoming, String
from kaiso.connection import get_connection
from kaiso.exceptions import (
    UniqueConstraintError, UnknownType, CannotUpdateType, UnsupportedTypeError,
    TypeNotPersistedError, NoResultFound)
from kaiso.queries import (
    get_create_types_query, get_create_relationship_query, get_start_clause,
    join_lines)
from kaiso.references import set_store_for_object
from kaiso.relationships import InstanceOf, IsA, DeclaredOn
from kaiso.serialize import (
    dict_to_db_values_dict, get_changes, object_to_db_value)
from kaiso.types import (
    INTERNAL_CLASS_ATTRS, Descriptor, Persistable, PersistableType,
    Relationship, TypeRegistry, AttributedBase, get_declaring_class,
    get_index_name, get_type_id, is_indexable)
from kaiso.utils import dict_difference


# Note: some Cypher queries contain dummy names for unused nodes, e.g. (dummy1)
# instead of (). This is an attempted workaround for an intermittent Cypher
# bug (https://github.com/neo4j/neo4j/issues/1040) and may be removed when
# used against later versions of Neo4j.


log = getLogger(__name__)


class TypeSystem(AttributedBase):
    """ ``TypeSystem`` is a node that represents the root
    of the type hierarchy.

    Inside the database, the current version of the hierarchy is
    tracked using a ``version`` attribute on the TypeSystem node.
    """
    id = String(unique=True)


def get_attr_filter(obj, type_registry):
    """ Generates a dictionary, that will identify the given ``obj``
    based on it's unique attributes.

    Args:
        obj: An object to look up by index

    Returns:
        A dictionary of key-value pairs to indentify an object by.
    """
    indexes = type_registry.get_index_entries(obj)
    index_filter = dict((key, value) for _, key, value in indexes)
    return index_filter


class Manager(object):
    """Manage the interface to graph-based queryable object store.

    The any object can be saved as long as its type is registered.
    This includes instances of Entity, PersistableType
    and subclasses of either.

    InstanceOf and IsA relationships are automatically generated
    when persisting an object.
    """
    _type_registry_cache = None

    def __init__(self, connection_uri, skip_type_loading=False):
        """ Initializes a Manager object.

        Args:
            connection_uri: A URI used to connect to the graph database.
        """
        self._conn = get_connection(connection_uri)
        self.type_system = TypeSystem(id='TypeSystem')
        self.type_registry = TypeRegistry()
        idx_name = get_index_name(TypeSystem)
        self._conn.get_or_create_index(neo4j.Node, idx_name)
        self.save(self.type_system)
        if not skip_type_loading:
            self.reload_types()

    def _execute(self, query, **params):
        """ Runs a cypher query returning only raw rows of data.

        Args:
            query: A parameterized cypher query.
            params: The parameters used by the query.

        Returns:
            A generator with the raw rows returned by the connection.
        """
        # 2.0 compatibility as we transition
        query = "CYPHER 1.9 {}".format(query)

        log.debug('running query:\n%s', query.format(**params))

        rows, _ = cypher.execute(self._conn, query, params)

        return (row for row in rows)

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
            properties = value._properties

            obj = self.type_registry.dict_to_object(properties)

            if isinstance(value, neo4j.Relationship):
                # prefetching start and end-nodes as they don't have
                # their properties loaded yet
                value.start_node.get_properties()
                value.end_node.get_properties()

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

        indexes = self.type_registry.get_index_entries(obj)
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

    def _type_system_version(self):
        query = join_lines(
            'START',
            get_start_clause(self.type_system, 'ts', self.type_registry),
            'RETURN ts.version?'
        )

        rows = self._execute(query)
        (version,) = next(rows)
        return version

    def invalidate_type_system(self):
        query = join_lines(
            'START',
            get_start_clause(self.type_system, 'ts', self.type_registry),
            'SET ts.version = {new_version}'
        )

        new_version = uuid.uuid4().hex
        next(self._execute(query, new_version=new_version), None)

    def reload_types(self):
        """Reload the type registry for this instance from the graph
        database.
        """
        current_version = self._type_system_version()
        if Manager._type_registry_cache:
            cached_registry, version = Manager._type_registry_cache
            if current_version == version:
                log.debug(
                    'using cached type registry, version: %s', current_version)
                self.type_registry = cached_registry.clone()
                return

        self.type_registry = TypeRegistry()
        registry = self.type_registry

        for type_id, bases, attrs in self.get_type_hierarchy():
            try:
                cls = registry.get_class_by_id(type_id)

                # static types also get loaded into dynamic registry
                # to allow them to be augmented
                if registry.is_static_type(cls):
                    cls = None
            except UnknownType:
                cls = None

            if cls is None:
                bases = tuple(registry.get_class_by_id(base) for base in bases)
                registry.create_type(str(type_id), bases, attrs)

            registry._types_in_db.add(type_id)

        Manager._type_registry_cache = (
            self.type_registry.clone(),
            current_version
        )

    def _get_changes(self, persistable):
        changes = {}
        existing = None
        obj_type = type(persistable)

        registry = self.type_registry

        if isinstance(persistable, PersistableType):
            # this is a class, we need to get it and it's attrs
            idx_name = get_index_name(PersistableType)
            self._conn.get_or_create_index(neo4j.Node, idx_name)

            type_id = get_type_id(persistable)
            query_args = {
                'type_id': type_id
            }

            query = join_lines(
                'START cls=node:%s(id={type_id})' % idx_name,
                'MATCH attr -[:DECLAREDON*0..]-> cls',
                'RETURN cls, collect(attr.name?)'
            )

            # don't use self.query since we don't want to convert the py2neo
            # node into an object
            rows = self._execute(query, **query_args)
            cls_node, attrs = next(rows, (None, None))

            if cls_node is None:
                # have not found the cls
                return None, {}

            existing_cls_attrs = cls_node._properties

            # Make sure we get a clean view of current data.
            registry.refresh_type(persistable)

            new_cls_attrs = registry.object_to_dict(persistable)

            # If any existing keys in "new" are missing in "old", add `None`s.
            # Unlike instance attributes, we just need to remove the properties
            # from the node, which we can achieve by setting the values to None
            for key in set(existing_cls_attrs) - set(new_cls_attrs):
                new_cls_attrs[key] = None
            changes = get_changes(old=existing_cls_attrs, new=new_cls_attrs)

            attrs = set(attrs)

            modified_attrs = {}

            descr = registry.get_descriptor(persistable)
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

            # we want to return the existing class
            existing = registry.get_descriptor_by_id(type_id).cls
        else:
            existing = self.get(obj_type, **get_attr_filter(persistable,
                                                            registry))
            if existing is not None:
                existing_props = registry.object_to_dict(existing)
                props = registry.object_to_dict(persistable)

                if existing_props == props:
                    return existing, {}

                changes = get_changes(old=existing_props, new=props)

        return existing, changes

    def _update_types(self, cls):
        query, objects, query_args = get_create_types_query(
            cls, self.type_system, self.type_registry)

        nodes_or_rels = next(self._execute(query, **query_args))

        for obj in objects:
            type_id = get_type_id(obj)
            self.type_registry._types_in_db.add(type_id)
            if is_indexable(obj):
                index_name = get_index_name(obj)
                self._conn.get_or_create_index(neo4j.Node, index_name)

        for obj, node_or_rel in zip(objects, nodes_or_rels):
            self._index_object(obj, node_or_rel)

        # we can't tell whether the CREATE UNIQUE from get_create_types_query
        # will have any effect, so we must invalidate.
        self.invalidate_type_system()
        return cls

    def _update(self, persistable, existing, changes):

        registry = self.type_registry

        for _, index_attr, _ in registry.get_index_entries(existing):
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

            query_args = {'type_id': get_type_id(persistable)}
            class_attr_changes = {k: v for k, v in changes.items()
                                  if k != 'attributes'}
            query_args.update(class_attr_changes)

            where = []

            descr = registry.get_descriptor(persistable)
            for attr_name in descr.declared_attributes.keys():
                where.append('attr.name = {attr_%s}' % attr_name)
                query_args['attr_%s' % attr_name] = attr_name

            if where:
                where = ' OR '.join(where)
                where = 'WHERE not(%s)' % where
            else:
                where = ''

            index_name = get_index_name(PersistableType)

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
            start_clause = get_start_clause(existing, 'n', registry)
            query = None

            if isinstance(persistable, Relationship):
                start_clauses = [start_clause]

                # if start or end have been set, we will do an index lookup
                # to reference them when "updating" the relationship to
                # point to them, if they are not set we look up the original
                # ones using a MATCH clause and "update" the relationship.
                new_start = getattr(persistable, 'start', None)
                new_end = getattr(persistable, 'end', None)

                if new_start is not None:
                    start_clauses.append(
                        get_start_clause(new_start, 'start_node', registry)
                    )
                    match_start_node = '(dummy1)'  # See note at top of page
                else:
                    match_start_node = 'start_node'

                if new_end is not None:
                    start_clauses.append(
                        get_start_clause(new_end, 'end_node', registry)
                    )
                    match_end_node = '(dummy2)'  # See note at top of page
                else:
                    match_end_node = 'end_node'

                start_clause = ', '.join(start_clauses)
                rel_props = registry.object_to_dict(persistable, for_db=True)

                query = join_lines(
                    'START %s' % start_clause,
                    'MATCH %s -[n]-> %s' % (match_start_node, match_end_node),
                    'DELETE n',
                    'CREATE start_node -[r:%s {rel_props}]-> end_node' % (
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
        invalidates_types = False

        if isinstance(obj, PersistableType):
            # object is a type; create the type and its hierarchy
            return self._update_types(obj)

        elif obj is self.type_system:
            query = 'CREATE (n {props}) RETURN n'

        elif isinstance(obj, Relationship):
            # object is a relationship
            obj_type = type(obj)

            if obj_type in (IsA, DeclaredOn):
                invalidates_types = True
            query = get_create_relationship_query(obj, self.type_registry)

        else:
            # object is an instance
            obj_type = type(obj)
            type_id = get_type_id(obj_type)
            if type_id not in self.type_registry._types_in_db:
                raise TypeNotPersistedError(type_id)

            idx_name = get_index_name(PersistableType)
            query = (
                'START cls=node:%s(id={type_id}) '
                'CREATE (n {props}) -[:INSTANCEOF {rel_props}]-> cls '
                'RETURN n'
            ) % idx_name

            query_args = {
                'type_id': get_type_id(obj_type),
                'rel_props': self.type_registry.object_to_dict(
                    InstanceOf(None, None), for_db=True),
            }

        query_args['props'] = self.type_registry.object_to_dict(
            obj, for_db=True)

        (node_or_rel,) = next(self._execute(query, **query_args))
        if invalidates_types:
            self.invalidate_type_system()
        self._index_object(obj, node_or_rel)

        return obj

    def get_type_hierarchy(self, start_type_id=None):
        """ Returns the entire type hierarchy defined in the database
        if start_type_id is None, else returns from that type.

        Returns: A generator yielding tuples of the form
        ``(type_id, bases, attrs)`` where
            - ``type_id`` identifies the type
            - ``bases`` lists the type_ids of the type's bases
            - ``attrs`` lists the attributes defined on the type
        """

        if start_type_id:
            # See note at top of page
            match = ('p=(ts -[:DEFINES]-> (dummy1) <-[:ISA*]- opt '
                     '<-[:ISA*0..]- tpe)')
            where = 'WHERE opt.id = {start_id}'
            query_args = {'start_id': start_type_id}
        else:
            # See note at top of page
            match = 'p=(ts -[:DEFINES]-> (dummy1) <-[:ISA*0..]- tpe)'
            where = ''
            query_args = {}

        query = join_lines(
            'START %s' % get_start_clause(self.type_system, 'ts',
                                          self.type_registry),
            'MATCH',
            match,
            where,
            ''' WITH tpe, max(length(p)) AS level
            MATCH
                tpe <-[?:DECLAREDON*]- attr,
                tpe -[isa?:ISA]-> base

            WITH tpe.id AS type_id, level, tpe AS class_attrs,
                filter(
                    idx_base in collect(DISTINCT [isa.base_index, base.id]):
                        not(LAST(idx_base) is NULL)
                ) AS bases,

                collect(DISTINCT attr) AS attrs

            ORDER BY level
            RETURN type_id, bases, class_attrs, attrs
            ''')

        # we can't use self.query since we don't want to convert the
        # class_attrs dict
        params = dict_to_db_values_dict(query_args)

        for row in self._execute(query, **params):
            type_id, bases, class_attrs, instance_attrs = row

            # the bases are sorted using their index on the IsA relationship
            bases = tuple(base for (_, base) in sorted(bases))
            class_attrs = class_attrs._properties
            for internal_attr in INTERNAL_CLASS_ATTRS:
                class_attrs.pop(internal_attr)
            instance_attrs = [self._convert_value(v) for v in instance_attrs]
            instance_attrs = {attr.name: attr for attr in instance_attrs}

            attrs = class_attrs
            attrs.update(instance_attrs)

            yield (type_id, bases, attrs)

    def serialize(self, obj, for_db=False):
        """ Serialize ``obj`` to a dictionary.

        Args:
            obj: An object to serialize
            for_db: (Optional) bool to indicate whether we are serializing
                data for neo4j or for general transport. This flag propagates
                down all the way into ``Attribute.to_primitive`` and may be
                used by custom attributes to determine behaviour for different
                serialisation targets. E.g. if using a transport that supports
                a Decimal type, `to_primitive` can return Decimal objects if
                for_db is False, and strings otherwise (for persistance in
                the neo4j db).

        Returns:
            A dictionary describing the object
        """
        return self.type_registry.object_to_dict(obj, for_db=for_db)

    def deserialize(self, object_dict):
        """ Deserialize ``object_dict`` to an object.

        Args:
            object_dict: A serialized object dictionary

        Returns:
            An object deserialized using the type registry
        """
        return self.type_registry.dict_to_object(object_dict)

    def create_type(self, name, bases, attrs):
        """ Creates a new class given the name, bases and attrs given.
        """
        return self.type_registry.create_type(name, bases, attrs)

    def update_type(self, tpe, bases):
        """ Change the bases of the given ``tpe``
        """
        if not isinstance(tpe, PersistableType):
            raise UnsupportedTypeError("Object is not a PersistableType")

        if self.type_registry.is_static_type(tpe):
            raise CannotUpdateType("Type '{}' is defined in code and cannot"
                                   "be updated.".format(get_type_id(tpe)))

        descriptor = self.type_registry.get_descriptor(tpe)
        existing_attrs = dict_difference(descriptor.attributes,
                                         descriptor.declared_attributes)
        base_attrs = {}
        for base in bases:
            desc = self.type_registry.get_descriptor(base)
            base_attrs.update(desc.attributes)
        base_attrs = dict_difference(base_attrs,
                                     descriptor.declared_attributes)

        if existing_attrs != base_attrs:
            raise CannotUpdateType("Inherited attributes are not identical")

        start_clauses = [get_start_clause(tpe, 'type', self.type_registry)]
        create_clauses = []
        query_args = {}

        for index, base in enumerate(bases):
            name = 'base_{}'.format(index)
            start = get_start_clause(base, name, self.type_registry)
            create = "type -[:ISA {%s_props}]-> %s" % (name, name)

            query_args["{}_props".format(name)] = {'base_index': index}
            start_clauses.append(start)
            create_clauses.append(create)

        query = join_lines(
            "START",
            (start_clauses, ','),
            "MATCH type -[r:ISA]-> (dummy1)",  # See note at top of page
            "DELETE r",
            "CREATE",
            (create_clauses, ','),
            "RETURN type")

        try:
            next(self._execute(query, **query_args))
            self.invalidate_type_system()
        except StopIteration:
            raise CannotUpdateType("Type or bases not found in the database.")

        self.reload_types()

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
        # we always want relationships to go through, even if there
        # are no changes in the properties, e.g. start or end have changed
        elif not changes and not isinstance(persistable, Relationship):
            return persistable
        else:
            return self._update(persistable, existing, changes)

    def save_collected_classes(self, collection):
        classes = collection.values()

        for cls in classes:
            self.type_registry.register(cls)

        for cls in classes:
            self.save(cls)

    def get(self, cls, **attr_filter):
        attr_filter = dict_to_db_values_dict(attr_filter)

        if not attr_filter:
            return None

        query_args = {}

        indexes = attr_filter.items()

        if issubclass(cls, (Relationship, PersistableType)):
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

        elif cls is TypeSystem:
            idx_name = get_index_name(TypeSystem)
            query = join_lines(
                'START ts=node:%s(id={idx_value})' % idx_name,
                'RETURN ts'
            )
            query_args['idx_value'] = self.type_system.id
        else:
            idx_where = []
            for key, value in indexes:
                idx_where.append('n.%s! = {%s}' % (key, key))
                query_args[key] = value
            idx_where = ' or '.join(idx_where)

            idx_name = get_index_name(TypeSystem)
            query = join_lines(
                'START root=node:%s(id={idx_value})' % idx_name,
                'MATCH ',
                '    n -[:INSTANCEOF]-> (dummy1)',   # See note at top of page
                '    -[:ISA*0..]-> tpe -[:ISA*0..]-> (dummy2) '
                '    <-[:DEFINES]- root',
                'WHERE %s' % idx_where,
                '   AND tpe.id = {tpe_id}',
                'RETURN n',
            )

            query_args['idx_value'] = self.type_system.id

            type_id = get_type_id(cls)
            query_args['tpe_id'] = type_id

        found = [node for (node,) in self._execute(query, **query_args)]

        if not found:
            return None

        # all the nodes returned should be the same
        first = found[0]
        for node in found:
            if node._id != first._id:
                raise UniqueConstraintError((
                    "Multiple nodes ({}) found for unique lookup for "
                    "{}").format(found, cls))

        obj = self._convert_value(first)
        return obj

    def get_by_unique_attr(self, cls, attr_name, values):
        """Bulk load entities from a list of values for a unique attribute

        Returns:
            A generator (obj1, obj2, ...) corresponding to the `values` list

        If any values are missing in the index, the corresponding obj is None
        """

        if not hasattr(cls, attr_name):
            raise ValueError("{} has no attribute {}".format(cls, attr_name))

        attr = getattr(cls, attr_name)
        if not attr.unique:
            raise ValueError("{}.{} is not unique".format(cls, attr_name))

        batch = neo4j.ReadBatch(self._conn)

        declaring_class = get_declaring_class(cls, attr_name)
        index_name = get_index_name(declaring_class)

        for value in values:
            db_value = object_to_db_value(value)
            batch.get_indexed_nodes(index_name, attr_name, db_value)

        # When upgrading to py2neo 1.6, consider changing this to batch.stream
        batch_result = batch.submit()

        def first_or_none(list_):
            return next(iter(list_), None)

        # `batch_result` is a list of either one element lists (for matches)
        # or empty lists. Unpack to flatten (and hydrate to Kaiso objects)
        result = (self._convert_value(
            first_or_none(row)) for row in batch_result)

        return result

    def change_instance_type(self, obj, type_id, updated_values=None):
        if updated_values is None:
            updated_values = {}

        type_registry = self.type_registry

        if type_id not in type_registry._types_in_db:
            raise TypeNotPersistedError(type_id)

        properties = self.serialize(obj, for_db=True)
        properties['__type__'] = type_id
        properties.update(updated_values)

        # get rid of any attributes not supported by the new type
        properties = self.serialize(self.deserialize(properties), for_db=True)

        tpe = type_registry.get_class_by_id(type_id)

        rel_props = type_registry.object_to_dict(InstanceOf(), for_db=True)

        start_clauses = (
            get_start_clause(obj, 'obj', type_registry),
            get_start_clause(tpe, 'tpe', type_registry)
        )

        # See note at top of page
        query = join_lines(
            'START',
            (start_clauses, ','),
            'MATCH (obj)-[old_rel:INSTANCEOF]->(dummy1)',
            'DELETE old_rel',
            'CREATE (obj)-[new_rel:INSTANCEOF {rel_props}]->(tpe)',
            'SET obj={properties}',
            'RETURN obj',
        )

        # use _execute; we need the raw object to add to the index
        results = self._execute(
            query, properties=properties, rel_props=rel_props)
        row = next(results, None)

        if row is None:
            raise NoResultFound(
                "{} not found in db".format(repr(obj))
            )

        (node,) = row
        new_obj = self._convert_value(node)

        # update any indexes
        old_indexes = set(type_registry.get_index_entries(obj))
        new_indexes = set(type_registry.get_index_entries(new_obj))
        indexes_to_remove = old_indexes - new_indexes
        indexes_to_add = new_indexes - old_indexes

        for index_name, key, value in indexes_to_remove:
            index = self._conn.get_index(neo4j.Node, index_name)
            index.remove(key, value)

        for index_name, key, value in indexes_to_add:
            index = self._conn.get_or_create_index(neo4j.Node, index_name)
            index.add(key, value, node)

        set_store_for_object(new_obj, self)

        return new_obj

    def get_related_objects(self, rel_cls, ref_cls, obj):

        if ref_cls is Outgoing:
            rel_query = 'n -[relation:{}]-> related'
        elif ref_cls is Incoming:
            rel_query = 'n <-[relation:{}]- related'

        # TODO: should get the rel name from descriptor?
        rel_query = rel_query.format(rel_cls.__name__.upper())

        query = join_lines(
            'START {idx_lookup} MATCH {rel_query}',
            'RETURN related, relation'
        )

        query = query.format(
            idx_lookup=get_start_clause(obj, 'n', self.type_registry),
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
        invalidates_types = False

        if isinstance(obj, Relationship):
            if is_indexable(type(obj)):
                query = join_lines(
                    'START',
                    get_start_clause(obj, 'rel', self.type_registry),
                    'DELETE rel',
                    'RETURN 0, count(rel)'
                )
            else:
                query = join_lines(
                    'START {}, {}',
                    'MATCH n1 -[rel]-> n2',
                    'DELETE rel',
                    'RETURN 0, count(rel)'
                ).format(
                    get_start_clause(obj.start, 'n1', self.type_registry),
                    get_start_clause(obj.end, 'n2', self.type_registry),
                )
            rel_type = type(obj)
            if rel_type in (IsA, DeclaredOn):
                invalidates_types = True

        elif isinstance(obj, PersistableType):
            query = join_lines(
                'START {}',
                'MATCH attr -[?:DECLAREDON]-> obj',
                'DELETE attr',
                'MATCH obj -[rel]- (dummy1)',  # See note at top of page
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_start_clause(obj, 'obj', self.type_registry)
            )
            invalidates_types = True
        else:
            query = join_lines(
                'START {}',
                'MATCH obj -[rel]- (dummy1)',  # See note at top of page
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_start_clause(obj, 'obj', self.type_registry)
            )

        # TODO: delete node/rel from indexes
        res = next(self._execute(query))
        if invalidates_types:
            self.invalidate_type_system()
        return res

    def query(self, query, **params):
        """ Queries the store given a parameterized cypher query.

        Args:
            query: A parameterized cypher query.
            params: query: A parameterized cypher query.

        Returns:
            A generator with tuples containing stored objects or values.

        WARNING: If you use this method to modify the type hierarchy (i.e.
        types, their declared attributes or their relationships), ensure
        to call ``manager.invalidate_type_hierarchy()`` afterwards.
        Otherwise managers will continue to use cached versions. Instances can
        be modified without changing the type hierarchy.
        """
        params = dict_to_db_values_dict(params)
        result = self._execute(query, **params)

        return (tuple(self._convert_row(row)) for row in result)

    def destroy(self):
        """ Removes all nodes, relationships and indexes in the store. This
            object will no longer be usable after calling this method.
            Construct a new Manager to re-initialise the database for kaiso.

            WARNING: This will destroy everything in your Neo4j database.

        """
        self._conn.clear()
        for index_name in self._conn.get_indexes(neo4j.Node).keys():
            self._conn.delete_index(neo4j.Node, index_name)
        for index_name in self._conn.get_indexes(neo4j.Relationship).keys():
            self._conn.delete_index(neo4j.Relationship, index_name)
