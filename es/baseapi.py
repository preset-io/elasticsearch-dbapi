from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from es import exceptions
from six import string_types
from six.moves.urllib import parse


from .const import DEFAULT_FETCH_SIZE, DEFAULT_SCHEMA, DEFAULT_SQL_PATH


CursorDescriptionRow = namedtuple(
    "CursorDescriptionRow",
    ["name", "type", "display_size", "internal_size", "precision", "scale", "null_ok"],
)

CursorDescriptionType = List[CursorDescriptionRow]


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3
    DATETIME = 4


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def wrap(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                "{klass} already closed".format(klass=self.__class__.__name__)
            )
        return f(self, *args, **kwargs)

    return wrap


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def wrap(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return wrap


def get_type(data_type) -> int:
    type_map = {
        "text": Type.STRING,
        "keyword": Type.STRING,
        "integer": Type.NUMBER,
        "half_float": Type.NUMBER,
        "scaled_float": Type.NUMBER,
        "geo_point": Type.STRING,
        # TODO get a solution for nested type
        "nested": Type.STRING,
        "object": Type.STRING,
        "date": Type.DATETIME,
        "datetime": Type.DATETIME,
        "timestamp": Type.DATETIME,
        "short": Type.NUMBER,
        "long": Type.NUMBER,
        "float": Type.NUMBER,
        "double": Type.NUMBER,
        "bytes": Type.NUMBER,
        "boolean": Type.BOOLEAN,
        "ip": Type.STRING,
        "interval_minute_to_second": Type.STRING,
        "interval_hour_to_second": Type.STRING,
        "interval_hour_to_minute": Type.STRING,
        "interval_day_to_second": Type.STRING,
        "interval_day_to_minute": Type.STRING,
        "interval_day_to_hour": Type.STRING,
        "interval_year_to_month": Type.STRING,
        "interval_second": Type.STRING,
        "interval_minute": Type.STRING,
        "interval_day": Type.STRING,
        "interval_month": Type.STRING,
        "interval_year": Type.STRING,
        "time": Type.STRING,
    }
    return type_map[data_type.lower()]


def get_description_from_columns(
    columns: List[Dict[str, str]]
) -> CursorDescriptionType:
    return [
        (
            CursorDescriptionRow(
                column.get("name") if not column.get("alias") else column.get("alias"),
                get_type(column.get("type")),
                None,  # [display_size]
                None,  # [internal_size]
                None,  # [precision]
                None,  # [scale]
                True,  # [null_ok]
            )
        )
        for column in columns
    ]


class BaseConnection(object):

    """Connection to an ES Cluster """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        path: str = "",
        scheme: str = "http",
        user: Optional[str] = None,
        password: Optional[str] = None,
        context: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ):
        netloc = f"{host}:{port}"
        path = path or "/"
        self.url = parse.urlunparse((scheme, netloc, path, None, None, None))
        self.context = context or {}
        self.closed = False
        self.cursors: List[BaseCursor] = []
        self.kwargs = kwargs
        # Subclass needs to initialize Elasticsearch
        self.es = None

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Elasticsearch doesn't support transactions.

        So just do nothing to support this method.
        """
        pass

    @check_closed
    def cursor(self):
        raise NotImplementedError  # pragma: no cover

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class BaseCursor:
    """Connection cursor."""

    custom_sql_to_method: Dict[str, str] = {}
    """
    Each child implements custom SQL commands so that we can
    add extra missing logic or restrictions.
    Maps custom SQL to class methods, cursor execute calls a dispatcher
    based on this mapping.
    """

    def __init__(self, url: str, es: Elasticsearch, **kwargs):
        """
        Base cursor constructor initializes common properties
        that are shared by opendistro and elastic. Child just
        override the sql_path since they differ on each distribution

        :param url: The connection URL
        :param es: An initialized Elasticsearch object
        :param kwargs: connection string query arguments
        """
        self.url = url
        self.es = es
        self.sql_path = kwargs.get("sql_path", DEFAULT_SQL_PATH)
        self.fetch_size = kwargs.get("fetch_size", DEFAULT_FETCH_SIZE)
        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated after a query
        self.description: CursorDescriptionType = []

        # this is set to an iterator after a successful query
        self._results: List[Tuple[Any, ...]] = []

    def custom_sql_to_method_dispatcher(self, command: str) -> Optional["BaseCursor"]:
        """
        Generic CUSTOM SQL dispatcher for internal methods
        :param command: str
        :return: None if no command found, or a Cursor with the result
        """
        method_name = self.custom_sql_to_method.get(command.lower())
        return getattr(self, method_name)() if method_name else None

    @property  # type: ignore
    @check_result
    @check_closed
    def rowcount(self) -> int:
        """ Counts the number of rows on a result """
        if self._results:
            return len(self._results)
        return 0

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None) -> "BaseCursor":
        """ Children must implement their own custom execute """
        raise NotImplementedError  # pragma: no cover

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead"
        )

    @check_result
    @check_closed
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        output, self._results = self._results[:size], self._results[size:]
        return output

    @check_result
    @check_closed
    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self)

    @check_closed
    def setinputsizes(self, sizes):  # pragma: no cover
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):  # pragma: no cover
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return self

    @check_closed
    def __next__(self):
        output = self.fetchone()
        if output is None:
            raise StopIteration
        return output

    next = __next__

    def sanitize_query(self, query: str) -> str:
        """
        Removes dummy schema from queries
        """
        return query.replace(f'FROM "{DEFAULT_SCHEMA}".', "FROM ")

    def elastic_query(self, query: str) -> Dict[str, Any]:
        """
        Request an http SQL query to elasticsearch
        """
        # Sanitize query
        query = self.sanitize_query(query)
        payload = {"query": query}
        if self.fetch_size is not None:
            payload["fetch_size"] = self.fetch_size
        path = f"/{self.sql_path}/"
        try:
            response = self.es.transport.perform_request("POST", path, body=payload)
        except es_exceptions.ConnectionError:
            raise exceptions.OperationalError(f"Error connecting to Elasticsearch")
        except es_exceptions.RequestError as ex:
            raise exceptions.ProgrammingError(
                f"Error ({ex.error}): {ex.info['error']['reason']}"
            )
        # Opendistro errors are http status 200
        if "error" in response:
            raise exceptions.ProgrammingError(
                f"({response['error']['reason']}): {response['error']['details']}"
            )
        return response


def apply_parameters(operation: str, parameters: Optional[Dict[str, Any]]) -> str:
    if parameters is None:
        return operation

    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value):
    if value == "*":
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)
