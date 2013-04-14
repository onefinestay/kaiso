from functools import wraps
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
            value = attr.to_db(getattr(obj, name))
            if value is not None:
                properties[name] = value
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


def unique(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        items = set()
        for item in fn(*args, **kwargs):
            if item not in items:
                items.add(item)
                yield item
    return wrapped

@unique
def get_type_relationships(obj):
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


def types_to_query(obj):
    lines = []
    objects = {'PersistableType': PersistableType}

    for obj1, rel_cls, obj2 in get_type_relationships(obj):
        if is_persistable_not_rel(obj1) and is_persistable_not_rel(obj2):
            if isinstance(obj1, type):
                name1 = obj1.__name__
            else:
                name1 = 'new_obj'

            name2 = obj2.__name__

            if name1 in objects:
                abstr1 = name1
            else:
                abstr1 = '(%s {%s_props})' % (name1, name1)

            if name2 in objects:
                abstr2 = name2
            else:
                abstr2 = '(%s {%s_props})' % (name2, name2)

            objects[name1] = obj1
            objects[name2] = obj2

            rel_name = rel_cls.__name__
            rel_type = rel_name.upper()

            ln = '%s -[:%s {%s_props}]-> %s' % (
                abstr1, rel_type, rel_name, abstr2)

            lines.append(ln)

    del objects['PersistableType']

    keys, objects = zip(*objects.items())

    query = (
        'START PersistableType=node:persistabletype(name="PersistableType")',
        'CREATE UNIQUE')
    query += ('    ' + ',\n    '.join(lines),)
    query += ('RETURN %s' % ', '.join(keys),)
    query = '\n'.join(query)

    return query, objects, keys


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

        try:
            assert self.get(PersistableType, name='PersistableType')
        except:
            self._add_obj(PersistableType)

        if obj is PersistableType:
            return
        elif isinstance(obj, Relationship):
            self._add_obj(obj)
            return

        query, objects, keys = types_to_query(obj)

        query_args = {
            'InstanceOf_props': object_to_dict(InstanceOf(None, None)),
            'IsA_props': object_to_dict(IsA(None, None))
        }

        for key, obj in zip(keys, objects):
            query_args['%s_props' % key] = object_to_dict(obj)

        nodes = list(self._execute(query, **query_args))[0]

        for node, obj in zip(nodes, objects):
            self._store_primitive(obj, node)

            indexes = get_indexes(obj)
            for index_name, key, value in indexes:
                index = self._conn.get_or_create_index(neo4j.Node, index_name)
                index.add(key, value, node)


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
        or issubclass(type(type(obj)), PersistableType)
    )



