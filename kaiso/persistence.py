from __future__ import unicode_literals

from logging import getLogger
import uuid

from py2neo import cypher, neo4j

from kaiso.attributes import Outgoing, Incoming, String
from kaiso.exceptions import (
    UnknownType, CannotUpdateType, UnsupportedTypeError,
    TypeNotPersistedError, NoResultFound, NoUniqueAttributeError)
from kaiso.queries import (
    get_create_types_query, get_create_relationship_query, get_match_clause,
    join_lines, parameter_map)
from kaiso.references import set_store_for_object
from kaiso.relationships import InstanceOf, IsA, DeclaredOn
from kaiso.serialize import (
    dict_to_db_values_dict, get_changes, object_to_db_value)
from kaiso.types import (
    INTERNAL_CLASS_ATTRS, Descriptor, Persistable, PersistableType,
    Relationship, TypeRegistry, AttributedBase, get_type_id,
    get_neo4j_relationship_name,
)
from kaiso.utils import dict_difference


log = getLogger(__name__)


def get_connection(uri):
    return neo4j.GraphDatabaseService(uri)


class TypeSystem(AttributedBase):
    """ ``TypeSystem`` is a node that represents the root
    of the type hierarchy.

    Inside the database, the current version of the hierarchy is
    tracked using a ``version`` attribute on the TypeSystem node.
    """
    id = String(unique=True)


class Manager(object):
    """Manage the interface to graph-based queryable object store.

    The any object can be saved as long as its type is registered.
    This includes instances of Entity, PersistableType
    and subclasses of either.

    InstanceOf and IsA relationships are automatically generated
    when persisting an object.
    """
    _type_registry_cache = None

    def __init__(self, connection_uri, skip_setup=False):
        """ Initializes a Manager object.

        Args:
            connection_uri: A URI used to connect to the graph database.
        """
        self._conn = get_connection(connection_uri)

        self.type_system = TypeSystem(id='TypeSystem')
        self.type_registry = TypeRegistry()

        if skip_setup:
            return

        batch = neo4j.WriteBatch(self._conn)
        batch.append_cypher("""
            CREATE CONSTRAINT ON (typesystem:TypeSystem)
            ASSERT typesystem.id IS UNIQUE
        """)
        batch.append_cypher("""
            CREATE CONSTRAINT ON (type:PersistableType)
            ASSERT type.id IS UNIQUE
        """)
        batch.run()
        # can't be in batch: "Cannot perform data updates in a transaction that
        # has performed schema updates"
        self.query('MERGE (ts:TypeSystem {id: "TypeSystem"})')
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
        query = "CYPHER 2.0 {}".format(query)

        log.debug('running query:\n%s\n\nwith params %s', query, params)

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
            properties = value._properties.copy()

            if isinstance(value, neo4j.Relationship):
                # inject __type__ based on the relationship type in case it's
                # missing. makes it easier to add relationship with cypher
                neo4j_rel_name = value.type
                type_id = self.type_registry.get_relationship_type_id(
                    neo4j_rel_name)
                properties['__type__'] = type_id

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

        elif isinstance(value, list):
            return [self._convert_value(v) for v in value]

        return value

    def _convert_row(self, row):
        for value in row:
            if isinstance(value, list):
                yield [self._convert_value(v) for v in value]
            else:
                yield self._convert_value(value)

    def _type_system_version(self):
        query = 'MATCH (ts:TypeSystem {id: "TypeSystem"}) RETURN ts.version'
        rows = self._execute(query)
        (version,) = next(rows)
        return version

    def invalidate_type_system(self):
        query = """
            MATCH (ts:TypeSystem {id: "TypeSystem"})
            SET ts.version = {new_version}
        """
        new_version = uuid.uuid4().hex
        self.query(query, new_version=new_version)

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

        registry = self.type_registry

        if isinstance(persistable, PersistableType):

            if issubclass(persistable, Relationship):
                # not stored in the db; must be static
                return None, {}

            query = """
                MATCH
                    {}
                OPTIONAL MATCH
                    (attr)-[:DECLAREDON*0..]->(cls)
                RETURN
                    cls, collect(attr.name)
            """.format(get_match_clause(persistable, 'cls', registry))

            # don't use self.query since we don't want to convert the py2neo
            # node into an object
            rows = self._execute(query)
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
            type_id = get_type_id(persistable)
            existing = registry.get_descriptor_by_id(type_id).cls
        else:
            try:
                query = 'MATCH {} RETURN obj'.format(
                    get_match_clause(persistable, 'obj', registry)
                )
            except NoUniqueAttributeError:
                existing = None
            else:
                existing = self.query_single(query)

            if existing is not None:
                existing_props = registry.object_to_dict(existing)
                props = registry.object_to_dict(persistable)

                if existing_props == props:
                    return existing, {}

                changes = get_changes(old=existing_props, new=props)

        return existing, changes

    def _update_types(self, cls):
        query, objects, query_args = get_create_types_query(
            cls, self.type_system.id, self.type_registry)

        self._execute(query, **query_args)

        for obj in objects:
            type_id = get_type_id(obj)
            self.type_registry._types_in_db.add(type_id)
            type_constraints = self.type_registry.get_constraints_for_type(obj)
            for constraint_type_id, constraint_attr_name in type_constraints:
                self.query(
                    """
                        CREATE CONSTRAINT ON (type:{type_id})
                        ASSERT type.{attr_name} IS UNIQUE
                    """.format(
                        type_id=constraint_type_id,
                        attr_name=constraint_attr_name,
                    )
                )

        # we can't tell whether the CREATE UNIQUE from get_create_types_query
        # will have any effect, so we must invalidate.
        self.invalidate_type_system()
        return cls

    def _update(self, persistable, existing, changes):

        registry = self.type_registry

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

            query = join_lines(
                'MATCH (n:PersistableType)',
                'WHERE n.id = {type_id}',
                set_clauses,
                'WITH n',
                'MATCH attr -[r:DECLAREDON]-> n',
                where,
                'DELETE attr, r',
                'RETURN n',
            )
            self._update_types(persistable)
        else:
            match_clause = get_match_clause(existing, 'n', registry)
            query = join_lines(
                'MATCH %s' % match_clause,
                set_clauses,
                'RETURN n'
            )
            query_args = changes

        try:
            (result,) = next(self._execute(query, **query_args))
        except StopIteration:
            # this can happen, if no attributes where changed on a type
            result = persistable

        return result

    def _add(self, obj):
        """ Adds an object to the data store.

        It will automatically generate the type relationships
        for the the object as required and store the object itself.
        """

        type_registry = self.type_registry
        query_args = {}
        invalidates_types = False

        if isinstance(obj, PersistableType):
            # object is a type; create the type and its hierarchy
            return self._update_types(obj)

        elif isinstance(obj, Relationship):
            # object is a relationship
            obj_type = type(obj)

            if obj_type in (IsA, DeclaredOn):
                invalidates_types = True
            query = get_create_relationship_query(obj, type_registry)

        else:
            # object is an instance
            obj_type = type(obj)
            type_id = get_type_id(obj_type)
            if type_id not in type_registry._types_in_db:
                raise TypeNotPersistedError(type_id)

            labels = type_registry.get_labels_for_type(obj_type)
            if labels:
                node_declaration = 'n:' + ':'.join(labels)
            else:
                node_declaration = 'n'

            query = """
                MATCH (cls:PersistableType)
                WHERE cls.id = {type_id}
                CREATE (%s {props})-[:INSTANCEOF {rel_props}]->(cls)
                RETURN n
            """ % node_declaration

            query_args = {
                'type_id': get_type_id(obj_type),
                'rel_props': type_registry.object_to_dict(
                    InstanceOf(None, None), for_db=True),
            }

        query_args['props'] = type_registry.object_to_dict(
            obj, for_db=True)

        (node_or_rel,) = next(self._execute(query, **query_args))
        if invalidates_types:
            self.invalidate_type_system()

        set_store_for_object(obj, self)
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
            match = """
                p = (
                    (ts:TypeSystem {id: "TypeSystem"})-[:DEFINES]->()<-
                        [:ISA*]-(opt)<-[:ISA*0..]-(tpe)
                )
                WHERE opt.id = {start_id}
                """
            query_args = {'start_id': start_type_id}
        else:
            match = """
                p=(
                    (ts:TypeSystem {id: "TypeSystem"})-[:DEFINES]->()<-
                        [:ISA*0..]-(tpe)
                )
                """
            query_args = {}

        query = join_lines(
            'MATCH',
            match,
            """
            WITH tpe, max(length(p)) AS level
            OPTIONAL MATCH
                tpe <-[:DECLAREDON*]- attr
            OPTIONAL MATCH
                tpe -[isa:ISA]-> base

            WITH tpe.id AS type_id, level, tpe AS class_attrs,
                filter(
                    idx_base in collect(DISTINCT [isa.base_index, base.id])
                    WHERE not(LAST(idx_base) is NULL)
                ) AS bases,

                collect(DISTINCT attr) AS attrs

            ORDER BY level
            RETURN type_id, bases, class_attrs, attrs
            """)

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

        match_clauses = [get_match_clause(tpe, 'type', self.type_registry)]
        create_clauses = []
        query_args = {}

        for index, base in enumerate(bases):
            name = 'base_{}'.format(index)
            match = get_match_clause(base, name, self.type_registry)
            create = "type -[:ISA {%s_props}]-> %s" % (name, name)

            query_args["{}_props".format(name)] = {'base_index': index}
            match_clauses.append(match)
            create_clauses.append(create)

        query = join_lines(
            "MATCH",
            (match_clauses, ','),
            ", type -[r:ISA]-> ()",
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

        # Workaround that allows nodes and relationships with no
        # attrs to be saved. `save` will cause this method to be called
        # with an empty attr_filter, and when it receives None, will add
        # a new object.
        if not attr_filter:
            return None

        type_registry = self.type_registry

        unique_attrs = [key for _, key in type_registry.get_unique_attrs(cls)]
        query_params = {
            key: value
            for key, value in attr_filter.items()
            if key in unique_attrs
            and value is not None
        }
        if not query_params:
            raise ValueError(
                'No relevant indexes found when calling get for class: {}'
                ' with filter {}'.format(cls, attr_filter)
            )

        labels = type_registry.get_labels_for_type(cls)
        # since we found an index, we have at least one label
        node_declaration = 'n:' + ':'.join(labels)

        params = parameter_map(attr_filter, 'params')
        return self.query_single(
            "MATCH (%s %s) RETURN n" % (node_declaration, params),
            params=attr_filter)

    def get_by_unique_attr(self, cls, attr_name, values):
        """Bulk load entities from a list of values for a unique attribute

        Returns:
            A generator (obj1, obj2, ...) corresponding to the `values` list

        If any values are missing in the index, the corresponding obj is None
        """
        if not hasattr(cls, attr_name):
            raise ValueError("{} has no attribute {}".format(cls, attr_name))

        registry = self.type_registry
        for declaring_cls, attr in registry.get_unique_attrs(cls):
            if attr == attr_name:
                break
        else:
            raise ValueError("{}.{} is not unique".format(cls, attr_name))

        type_id = get_type_id(cls)
        query = "MATCH (n:%(label)s {%(attr)s: {id}}) RETURN n" % {
            'label': type_id,
            'attr': attr_name,
        }
        batch = neo4j.ReadBatch(self._conn)
        for value in values:
            db_value = object_to_db_value(value)
            batch.append_cypher(query, params={'id': db_value})

        # When upgrading to py2neo 1.6, consider changing this to batch.stream
        batch_result = batch.submit()

        # `batch_result` is a list of either one element lists (for matches)
        # or empty lists. Unpack to flatten (and hydrate to Kaiso objects)
        result = (self._convert_value(row) for row in batch_result)

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

        old_type = type(obj)
        new_type = type_registry.get_class_by_id(type_id)

        rel_props = type_registry.object_to_dict(InstanceOf(), for_db=True)

        old_labels = set(type_registry.get_labels_for_type(old_type))
        new_labels = set(type_registry.get_labels_for_type(new_type))
        removed_labels = old_labels - new_labels
        added_labels = new_labels - old_labels

        if removed_labels:
            remove_labels_statement = 'REMOVE obj:' + ':'.join(removed_labels)
        else:
            remove_labels_statement = ''

        if added_labels:
            add_labels_statement = 'SET obj :' + ':'.join(added_labels)
        else:
            add_labels_statement = ''

        match_clauses = (
            get_match_clause(obj, 'obj', type_registry),
            get_match_clause(new_type, 'type', type_registry)
        )

        query = join_lines(
            'MATCH',
            (match_clauses, ','),
            ', (obj)-[old_rel:INSTANCEOF]->()',
            'DELETE old_rel',
            'CREATE (obj)-[new_rel:INSTANCEOF {rel_props}]->(type)',
            'SET obj={properties}',
            remove_labels_statement,
            add_labels_statement,
            'RETURN obj',
        )

        new_obj = self.query_single(
            query, properties=properties, rel_props=rel_props)

        if new_obj is None:
            raise NoResultFound(
                "{} not found in db".format(repr(obj))
            )

        set_store_for_object(new_obj, self)
        return new_obj

    def get_related_objects(self, rel_cls, ref_cls, obj):

        if ref_cls is Outgoing:
            rel_query = '(n)-[relation:{}]->(related)'
        elif ref_cls is Incoming:
            rel_query = '(n)<-[relation:{}]-(related)'

        # TODO: should get the rel name from descriptor?
        rel_query = rel_query.format(get_neo4j_relationship_name(rel_cls))

        query = join_lines(
            'MATCH {idx_lookup}, {rel_query}'
            'RETURN related, relation'
        )

        query = query.format(
            idx_lookup=get_match_clause(obj, 'n', self.type_registry),
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
            query = join_lines(
                'MATCH {}, {},',
                'n1 -[rel]-> n2',
                'DELETE rel',
                'RETURN 0, count(rel)'
            ).format(
                get_match_clause(obj.start, 'n1', self.type_registry),
                get_match_clause(obj.end, 'n2', self.type_registry),
            )
            rel_type = type(obj)
            if rel_type in (IsA, DeclaredOn):
                invalidates_types = True

        elif isinstance(obj, PersistableType):
            query = join_lines(
                'MATCH {}',
                'OPTIONAL MATCH attr -[:DECLAREDON]-> obj',
                'DELETE attr',
                'WITH obj',
                'MATCH obj -[rel]- ()',
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_match_clause(obj, 'obj', self.type_registry)
            )
            invalidates_types = True
        else:
            query = join_lines(
                'MATCH {},',
                'obj -[rel]- ()',
                'DELETE obj, rel',
                'RETURN count(obj), count(rel)'
            ).format(
                get_match_clause(obj, 'obj', self.type_registry)
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

    def query_single(self, query, **params):
        """Convenience method for queries that return a single item"""
        rows = self.query(query, **params)
        for (item,) in rows:
            return item

    def destroy(self):
        """ Removes all nodes, relationships and indexes in the store. This
            object will no longer be usable after calling this method.
            Construct a new Manager to re-initialise the database for kaiso.

            WARNING: This will destroy everything in your Neo4j database.

        """
        self._conn.clear()
        # NB. we assume all indexes are from constraints (only use-case for
        # kaiso) if any aren't, this will not work
        batch = neo4j.WriteBatch(self._conn)
        for label in self._conn.node_labels:
            for key in self._conn.schema.get_indexed_property_keys(label):
                batch.append_cypher(
                    """
                        DROP CONSTRAINT ON (type:{type_id})
                        ASSERT type.{attr_name} IS UNIQUE
                    """.format(
                        type_id=label,
                        attr_name=key,
                    )
                )
        batch.run()
