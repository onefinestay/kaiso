""" Provides all the exceptions that may be raised.
"""


class ConnectionError(Exception):
    """ Raised when an error occurs connecting to the Neo4j Database
    """


class UniqueConstraintError(Exception):
    """Raised when attempting to create more than one object with the same
       value on an attribute declared to be unique"""


class MultipleObjectsFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected a single object, but multiple were returned.
    """


class NoResultFound(Exception):
    """ Raised when a caller of a RelationshipManager
    expected at least one object, but none was found.
    """


class UnknownType(Exception):
    """ Raised when trying to deserialise a class that hasn't been
    registered
    """


class DeserialisationError(Exception):
    """ Raised when trying to deserialise a dict with no __type__ key """


class TypeAlreadyRegistered(Exception):
    pass


class TypeAlreadyCollected(Exception):
    pass


class CannotUpdateType(Exception):
    """ Raised when trying to update a type defined in code """


class UnsupportedTypeError(Exception):
    """ Raised when trying to interact with a non-Persistable type """
