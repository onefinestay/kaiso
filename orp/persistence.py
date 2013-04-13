from weakref import WeakKeyDictionary, WeakValueDictionary

from py2neo import cypher, neo4j

from orp.connection import get_connection
from orp.descriptors import (
    get_descriptor, get_named_descriptor, get_index_name)
from orp.query import encode_query_values
from orp.types import PersistableType, Persistable
from orp.relationships import Relationship, InstanceOf, IsA


def get_indexes(obj):

    if isinstance(obj, type):
        if issubclass(obj, PersistableType):
            obj_type = obj
        else:
            obj_type = type(obj)
        index_name = get_index_name(obj_type)
        value = get_descriptor(obj).type_name
        yield (index_name, 'name', value)
    else:
        descr = get_descriptor(type(obj))

        for name, attr in descr.members.items():
            if attr.unique:
                index_name = get_index_name(attr.declared_on)
                key = name
                value = attr.to_db(getattr(obj, name))
                yield (index_name, key, value)


def object_to_dict(obj):

    if isinstance(obj, type) and issubclass(obj, PersistableType):
        obj_type = obj
    else:
        obj_type = type(obj)

    descr = get_descriptor(obj_type)

    properties = {
        '__type__': descr.type_name,
    }

    if isinstance(obj, type):
        properties['name'] = get_descriptor(obj).type_name
    else:
        for name, attr in descr.members.items():
            properties[name] = attr.to_db(getattr(obj, name))

    return properties


def dict_to_object(properties):
    type_name = properties['__type__']
    descriptor = get_named_descriptor(type_name)

    cls = descriptor.cls

    if issubclass(cls, PersistableType):
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


class Storage(object):
    def __init__(self, conn_uri):
        self._conn = get_connection(conn_uri)
        self._init_caches()

    def _init_caches(self):
        # keep track of this manager's objects so we can guarantee
        # object identity [that is, if n1 and n2 are model instances
        # that represent the same node, (n1 is n2) is True]
        self._primitives = WeakKeyDictionary()
        self._relationships = WeakValueDictionary()
        self._nodes = WeakValueDictionary()

    def _store_primitive(self, persistable, primitive):
        self._primitives[persistable] = primitive

        if isinstance(primitive, neo4j.Node):
            id_map = self._nodes
        elif isinstance(primitive, neo4j.Relationship):
            id_map = self._relationships

        id_map[primitive.id] = persistable

    def _add_obj(self, obj):
        properties = object_to_dict(obj)

        if obj in self._primitives:
            return

        # TODO: enforce uniqueness?

        if isinstance(obj, Relationship):
            # TODO: ensure that start and end exist
            n1 = self._primitives[obj.start]
            n2 = self._primitives[obj.end]

            cypher_rel_type = properties['__type__'].upper()

            (relationship,) = self._conn.create(
                (n1, cypher_rel_type, n2, properties))

            primitive = relationship
        else:
            # makes a node since properties is a dict
            # (a tuple makes relationships)
            (node,) = self._conn.create(properties)

            indexes = get_indexes(obj)
            for index_name, key, value in indexes:
                index = self._conn.get_or_create_index(neo4j.Node, index_name)
                index.add(key, value, node)

            primitive = node

        self._store_primitive(obj, primitive)

    def _execute(self, query, **params):
        """Run a query on this graph connection. Query and params are passed
           straight though to cypher.execute"""
        rows, _ = cypher.execute(self._conn, query, params)
        for row in rows:
            yield row

    def _convert_value(self, value):
        """Take a py2neo primitive value and return an OGM instance. If the
           given value is not a a Node or Relationship, return it unchanged
        """
        if isinstance(value, neo4j.Node):
            '''obj = self._nodes.get(value.id)
            if obj:
                return obj
            '''

            properties = value.get_properties()
            obj = dict_to_object(properties)

            self._store_primitive(obj, value)

            return obj

        elif isinstance(value, neo4j.Relationship):
            '''rel = self._relationships.get(value.id)
            if rel:
                return rel
            '''
            properties = value.get_properties()

            rel = dict_to_object(properties)

            rel.start = self._convert_value(value.start_node)
            rel.end = self._convert_value(value.end_node)

            self._store_primitive(rel, value)
            return rel

        return value

    def _convert_row(self, row):
        for value in row:
            yield self._convert_value(value)

    def get(self, cls, **index_filter):
        index_filter = encode_query_values(index_filter)
        descriptor = get_descriptor(cls)

        assert len(index_filter) == 1, "only one index allowed at a time"
        key, value = index_filter.items()[0]

        index_name = descriptor.get_index_name_for_attribute(key)
        node = self._conn.get_indexed_node(index_name, key, value)
        if node is None:
            return None

        obj = self._convert_value(node)
        return obj

    def query(self, query, **params):
        params = encode_query_values(params)
        for row in self._execute(query, **params):
            yield tuple(self._convert_row(row))

    def add(self, obj):
        if not is_persistable(obj):
            raise TypeError('cannot persist %s' % obj)

        self._add_obj(obj)

        if obj is PersistableType:
            obj_type = PersistableType
        else:
            obj_type = type(obj)

        if is_persistable_not_rel(obj):
            if obj is not obj_type:
                self.add(obj_type)

            instance_rel = InstanceOf(obj, obj_type)
            self._add_obj(instance_rel)

            if isinstance(obj, type):
                for base in obj.__bases__:
                    if is_persistable(base):
                        self.add(base)
                        is_a_rel = IsA(obj, base)
                        self._add_obj(is_a_rel)


def is_persistable_not_rel(obj):
    if isinstance(obj, Relationship):
        return False
    elif isinstance(obj, type) and issubclass(obj, Relationship):
        return False
    else:
        return is_persistable(obj)


def is_persistable(obj):
    return bool(
        isinstance(obj, (Persistable, PersistableType)) or
        (isinstance(obj, type) and issubclass(obj, PersistableType))
    )



