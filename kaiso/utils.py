def dict_difference(d1, d2):
    """ Returns the difference between two dictionaries.

    Keys and values must be equal.
    """
    return dict(((k, v) for (k, v) in d1.iteritems()
                 if k not in d2 or d2[k] != v))
