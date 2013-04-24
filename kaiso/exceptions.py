""" Provides all the exceptions that may be raised.
"""


class UniqueConstraintError(Exception):
    """Raised when attempting to create more than one object with the same
       value on an attribute declared to be unique"""


class NoIndexesError(Exception):
    """Raised when attempting to make use of unique attributes but none were
       declared"""


class MultipleObjectsFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected a single object, but multiple were returned.
    """


class NoResultFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected at least one object, but none was found.
    """


class UnknownEntityType(Exception):
    """ Raised when trying to deserialise a class that hasn't been
    registered
    """
