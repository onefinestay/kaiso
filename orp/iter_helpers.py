from functools import wraps


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

