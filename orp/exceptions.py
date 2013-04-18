""" Provides all the exceptions that may be raised.
"""

class UniqueConstraintError(Exception):
    pass


class NoIndexesError(Exception):
    pass


class MultipleObjectsFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected a single object, but multiple were returned.
    """

class NoResultFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected at least one object, but none was found.
    """
