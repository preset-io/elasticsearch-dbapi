from collections import namedtuple
from typing import Dict, List, Optional, Tuple

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


def get_type(data_type):
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
        host="localhost",
        port=9200,
        path="",
        scheme="http",
        user=None,
        password=None,
        context=None,
        **kwargs,
    ):
        netloc = f"{host}:{port}"
        path = path or "/"
        self.url = parse.urlunparse((scheme, netloc, path, None, None, None))
        self.context = context or {}
        self.closed = False
        self.cursors = []
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


class BaseCursor(object):

    """Connection cursor."""

    def __init__(self, url, es, **kwargs):
        self.url = url
        self.es = es
        self.sql_path = kwargs.get("sql_path") or DEFAULT_SQL_PATH
        self.fetch_size = kwargs.get("fetch_size") or DEFAULT_FETCH_SIZE
        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to an iterator after a successful query
        self._results = None

    @property  # type: ignore
    @check_result
    @check_closed
    def rowcount(self) -> int:
        return len(self._results)

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None) -> "BaseCursor":
        raise NotImplementedError  # pragma: no cover

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead"
        )

    @check_result
    @check_closed
    def fetchone(self) -> Optional[Tuple[str]]:
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
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[str]]:
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
    def fetchall(self) -> List[Tuple[str]]:
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

    def sanitize_query(self, query):
        # remove dummy schema from queries
        return query.replace(f'FROM "{DEFAULT_SCHEMA}".', "FROM ")

    def elastic_query(self, query: str):
        """
        Request an http SQL query to elasticsearch
        """
        self.description = None
        # Sanitize query
        query = self.sanitize_query(query)
        payload = {"query": query, "fetch_size": self.fetch_size}
        path = f"/{self.sql_path}/"
        try:
            response = self.es.transport.perform_request("POST", path, body=payload)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}"
            )
        except es_exceptions.RequestError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}"
            )
        # Opendistro errors are http status 200
        if "error" in response:
            raise exceptions.ProgrammingError(
                f"({response['error']['reason']}): {response['error']['details']}"
            )
        return response


def apply_parameters(operation, parameters):
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
