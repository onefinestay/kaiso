from weakref import WeakKeyDictionary


_object_storage_map = WeakKeyDictionary()


def set_store_for_object(obj, store):
    _object_storage_map[obj] = store


def get_store_for_object(obj):
    return _object_storage_map[obj]
