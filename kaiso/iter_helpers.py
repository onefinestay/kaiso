from functools import wraps


def unique(fn):
    """ Wraps a function to return only unique items.
    The wrapped function must return an iterable object.
    When the wrapped function is called, each item from the iterable
    will be yielded only once and duplicates will be ignored.

    Args:
        fn: The function to be wrapped.

    Returns:
        A wrapper function for fn.
    """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        items = set()
        for item in fn(*args, **kwargs):
            if item not in items:
                items.add(item)
                yield item
    return wrapped
