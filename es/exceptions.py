from typing import Any, Dict, Optional


class Error(Exception):
    """Base exception"""


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    # The SQL plugin's error body when it answered the error with HTTP 200
    response: Optional[Dict[str, Any]] = None


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class UnexpectedESInitError(Error):
    """Should never happen, when a cursor is requested
    without an ElasticSearch object being initialized"""


class UnexpectedRequestResponse(Error):
    """When perform request returns False, only when HTTP method HEAD
    and status code 404"""
