import six

def dict_difference(d1, d2):
    """ Returns the difference between two dictionaries.

    Keys and values must be equal.
    """
    sentinel = object()
    return dict(((k, v) for (k, v) in six.iteritems(d1)
                 if d2.get(k, sentinel) != v))
