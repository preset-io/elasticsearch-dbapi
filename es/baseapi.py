from collections import namedtuple
from contextlib import contextmanager
import datetime
import logging
import re
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib import parse

from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions
from es import exceptions
from opensearchpy import exceptions as os_exceptions
from opensearchpy import OpenSearch

from .const import DEFAULT_FETCH_SIZE, DEFAULT_SCHEMA, DEFAULT_SQL_PATH

logger = logging.getLogger(__name__)


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


_AUTH_ERRORS = (
    es_exceptions.AuthenticationException,
    es_exceptions.AuthorizationException,
    os_exceptions.AuthenticationException,
    os_exceptions.AuthorizationException,
)
_QUERY_ERRORS = (
    es_exceptions.RequestError,
    es_exceptions.NotFoundError,
    os_exceptions.RequestError,
    os_exceptions.NotFoundError,
)


_TRUE_VALUES = ("true", "1", "yes", "on")
_FALSE_VALUES = ("false", "0", "no", "off")


def parse_bool_argument(value: str) -> bool:
    """
    Parses a boolean connection argument, which arrives from a URL as a
    string: ``true``/``1``/``yes``/``on`` and ``false``/``0``/``no``/``off``,
    in any case.
    """
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Expected boolean found {value}")


def _describe(ex: Exception) -> str:
    # client exceptions built without the usual arguments cannot be str()'d
    try:
        return str(ex)
    except Exception:  # noqa: B902
        return type(ex).__name__


@contextmanager
def translate_transport_errors() -> Iterator[None]:
    """
    Re-raises client transport errors as this module's DB-API exceptions.

    Callers (SQLAlchemy, Superset) only recognise DB-API exceptions; a raw
    ``AuthenticationException`` from a wrong password would otherwise escape
    as an unrelated exception type. The original error stays chained and its
    message is kept, so a TLS failure still reads as a certificate problem.
    """
    try:
        yield
    except (es_exceptions.ConnectionError, os_exceptions.ConnectionError) as ex:
        raise exceptions.OperationalError(
            f"Error connecting to Elasticsearch/OpenSearch: {_describe(ex)}"
        ) from ex
    except _AUTH_ERRORS as ex:
        raise exceptions.OperationalError(f"Error ({ex.error}): {ex.info}") from ex
    except _QUERY_ERRORS as ex:
        raise exceptions.ProgrammingError(f"Error ({ex.error}): {ex.info}") from ex
    except (es_exceptions.TransportError, os_exceptions.TransportError) as ex:
        raise exceptions.DatabaseError(f"Error ({ex.error}): {ex.info}") from ex


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


_TYPE_MAP: Dict[str, int] = {
    "text": Type.STRING,
    "keyword": Type.STRING,
    "constant_keyword": Type.STRING,
    "wildcard": Type.STRING,
    "match_only_text": Type.STRING,
    "string": Type.STRING,
    "version": Type.STRING,
    "binary": Type.STRING,
    "null": Type.STRING,
    "undefined": Type.STRING,
    "unsupported": Type.STRING,
    "integer": Type.NUMBER,
    "half_float": Type.NUMBER,
    "scaled_float": Type.NUMBER,
    "geo_point": Type.STRING,
    "geo_shape": Type.STRING,
    "shape": Type.STRING,
    # TODO get a solution for nested type
    "nested": Type.STRING,
    "object": Type.STRING,
    "struct": Type.STRING,
    "array": Type.STRING,
    "date": Type.DATETIME,
    "datetime": Type.DATETIME,
    "timestamp": Type.DATETIME,
    "time": Type.DATETIME,
    "byte": Type.NUMBER,
    "short": Type.NUMBER,
    "long": Type.NUMBER,
    "unsigned_long": Type.NUMBER,
    "float": Type.NUMBER,
    "double": Type.NUMBER,
    "bytes": Type.NUMBER,
    "boolean": Type.BOOLEAN,
    "ip": Type.STRING,
    "interval": Type.STRING,
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


def get_type(data_type: Optional[str]) -> int:
    """
    Maps an Elasticsearch/OpenSearch SQL result type to a DB-API type code.

    Types this driver does not know about are reported as strings rather than
    failing the whole query: the server already produced the rows, and a new
    server version adding a type must not turn every query touching it into
    a crash.
    """
    type_code = _TYPE_MAP.get((data_type or "").lower())
    if type_code is None:
        logger.warning("Unknown result type %s, reporting it as a string", data_type)
        return Type.STRING
    return type_code


# Fractional seconds beyond microseconds cannot be represented by
# ``datetime``; values carrying them are returned as the server's string.
_FRACTION_RE = re.compile(r"\.(\d+)")
_UTC_SUFFIX_RE = re.compile(r"(?i)z$")


def _split_fraction(value: str) -> Optional[str]:
    """
    Normalizes the fractional seconds of an ISO-8601 string to exactly six
    digits so ``fromisoformat`` parses it on every supported Python version.
    Returns ``None`` if the value has non-zero digits past the microsecond,
    which a ``datetime`` cannot hold without losing precision.
    """
    match = _FRACTION_RE.search(value)
    if not match:
        return value
    digits = match.group(1)
    if len(digits) > 6 and digits[6:].strip("0"):
        return None
    digits = (digits + "000000")[:6]
    head, tail = value[: match.start()], value[match.end() :]  # noqa: E203
    return f"{head}.{digits}{tail}"


def _parse_iso(value: str) -> Optional[str]:
    normalized = _split_fraction(value.strip())
    if normalized is None:
        return None
    # ``fromisoformat`` only accepts a literal ``Z`` from Python 3.11 on
    return _UTC_SUFFIX_RE.sub("+00:00", normalized.replace(" ", "T", 1))


def parse_datetime(value: Any) -> Any:
    """
    Converts a DATETIME/TIMESTAMP value to ``datetime.datetime``.

    Elasticsearch renders an explicit offset (``Z`` or the session
    ``time_zone``), which is kept as ``tzinfo``; OpenSearch renders naive UTC
    wall time, which stays naive. Anything that cannot be represented exactly
    is returned unchanged.
    """
    if not isinstance(value, str):
        return value
    normalized = _parse_iso(value)
    if normalized is None:
        return value
    try:
        return datetime.datetime.fromisoformat(normalized)
    except ValueError:
        return value


def parse_date(value: Any) -> Any:
    """
    Converts a DATE value to ``datetime.date``. Elasticsearch renders DATE as
    midnight with an offset, OpenSearch as ``yyyy-MM-dd``.
    """
    if not isinstance(value, str):
        return value
    if len(value) == 10:
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            return value
    parsed = parse_datetime(value)
    if isinstance(parsed, datetime.datetime) and parsed.time() == datetime.time(0):
        return parsed.date()
    return value


def parse_time(value: Any) -> Any:
    """Converts a TIME value to ``datetime.time``, keeping any offset."""
    if not isinstance(value, str):
        return value
    normalized = _parse_iso(value)
    if normalized is None:
        return value
    try:
        return datetime.time.fromisoformat(normalized)
    except ValueError:
        return value


_VALUE_CONVERTERS: Dict[str, Callable[[Any], Any]] = {
    "datetime": parse_datetime,
    "timestamp": parse_datetime,
    "date": parse_date,
    "time": parse_time,
}


def convert_rows(
    columns: List[Dict[str, str]], rows: List[Tuple[Any, ...]]
) -> List[Tuple[Any, ...]]:
    """
    Converts temporal values, which both SQL endpoints serialize as JSON
    strings, into the Python objects matching each column's reported type.

    The decision is made per column: if any value of a column cannot be
    converted without loss (e.g. nanoseconds, which ``datetime`` cannot
    hold), the whole column keeps the server's strings, so a column never
    mixes strings and datetimes.
    """
    if not rows:
        return rows
    converted_columns: Dict[int, List[Any]] = {}
    for index, column in enumerate(columns):
        converter = _VALUE_CONVERTERS.get((column.get("type") or "").lower())
        if converter is None:
            continue
        values = [row[index] for row in rows]
        converted = [converter(v) if v is not None else v for v in values]
        if any(isinstance(o, str) and n is o for o, n in zip(values, converted)):
            logger.warning(
                "Column %s keeps string values: not all are representable",
                column.get("name"),
            )
            continue
        converted_columns[index] = converted
    if not converted_columns:
        return rows
    return [
        tuple(
            converted_columns[index][row_index] if index in converted_columns else value
            for index, value in enumerate(row)
        )
        for row_index, row in enumerate(rows)
    ]


def get_description_from_columns(
    columns: List[Dict[str, str]],
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
    """Connection to an ES Cluster"""

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
        # Subclass needs to initialize Elasticsearch or OpenSearch
        self.es: Optional[Union[Elasticsearch, OpenSearch]] = None

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

    def __init__(self, url: str, es: Union[Elasticsearch, OpenSearch], **kwargs):
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
        self.time_zone: Optional[str] = kwargs.get("time_zone")
        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated after a query
        self.description: CursorDescriptionType = []

        # this is set to an iterator after a successful query
        self._results: List[Tuple[Any, ...]] = []

    def empty_index_names(self) -> Set[str]:
        """
        Names of indices holding no documents, which table listings leave out
        because SQLAlchemy cannot reflect a table without columns.

        Reading index stats needs the ``monitor`` privilege, which a user
        granted only what SQL queries need does not have. Such a user can
        still query and reflect every index, so the listing must not fail
        for them: nothing is filtered out instead.
        """
        try:
            indices = self.es.cat.indices(format="json")
        except _AUTH_ERRORS as ex:
            logger.warning(
                "Not allowed to read index stats (%s); empty indices are listed",
                ex.error,
            )
            return set()
        return {
            item["index"]
            for item in cast(List[Dict[str, Any]], list(indices))
            if int(item.get("docs.count") or 0) == 0
        }

    def custom_sql_to_method_dispatcher(self, command: str) -> Optional["BaseCursor"]:
        """
        Generic CUSTOM SQL dispatcher for internal methods
        :param command: str
        :return: None if no command found, or a Cursor with the result
        """
        method_name = self.custom_sql_to_method.get(command.lower())
        return getattr(self, method_name)() if method_name else None

    @property
    @check_result
    @check_closed
    def rowcount(self) -> int:
        """Counts the number of rows on a result"""
        if self._results:
            return len(self._results)
        return 0

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None) -> "BaseCursor":
        """Children must implement their own custom execute"""
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

    def elastic_query(self, query: str, paged: bool = True) -> Dict[str, Any]:
        """
        Request an http SQL query to elasticsearch

        :param paged: send ``fetch_size`` so the result can be paged with a
            cursor. Only a caller that checks the result for truncation may
            turn it off.
        """
        # Sanitize query
        query = self.sanitize_query(query)
        payload: Dict[str, Any] = {"query": query}
        if paged and self.fetch_size is not None:
            payload["fetch_size"] = self.fetch_size
        if self.time_zone is not None:
            payload["time_zone"] = self.time_zone
        return self._sql_request(f"/{self.sql_path}/", payload)

    def elastic_cursor_query(self, cursor: str) -> Dict[str, Any]:
        """
        Follow an SQL cursor to fetch the next page of a result set.
        """
        return self._sql_request(f"/{self.sql_path}/", {"cursor": cursor})

    def close_elastic_cursor(self, cursor: str) -> None:
        """
        Release server-side resources held by an open SQL cursor.
        Best-effort: a failure here is logged, not raised, so it
        doesn't mask the original query result or error.
        """
        try:
            self._sql_request(f"/{self.sql_path}/close", {"cursor": cursor})
        except Exception:
            logger.warning("Failed to close SQL cursor", exc_info=True)

    def fetch_remaining_pages(
        self, response: Dict[str, Any], rows_key: str
    ) -> List[Tuple[Any, ...]]:
        """
        Returns the rows of ``response`` plus every page that follows it.

        Both SQL endpoints return at most ``fetch_size`` rows per request and
        hand back a ``cursor`` while more rows remain; stopping at the first
        page silently truncates the result set. The cursor is followed until
        a page comes back without one, which is the documented signal that
        the result set is exhausted (the server closes it itself), or until
        a page comes back empty.
        """
        rows = [tuple(row) for row in response.get(rows_key) or []]
        sql_cursor = response.get("cursor")
        try:
            while sql_cursor:
                page = self.elastic_cursor_query(sql_cursor)
                page_rows = page.get(rows_key) or []
                sql_cursor = page.get("cursor")
                if not page_rows:
                    # Nothing can follow an empty page; stop even if the
                    # server still hands back a cursor, which would otherwise
                    # loop forever. (A repeated cursor is no such signal:
                    # Elasticsearch returns the same cursor for every page of
                    # a result.)
                    break
                rows.extend(tuple(row) for row in page_rows)
        finally:
            # A cursor is only still open here if pagination was aborted by
            # an exception or stopped on an empty page; release it so the
            # server does not keep it alive
            if sql_cursor:
                self.close_elastic_cursor(sql_cursor)
        return rows

    def _sql_request(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"body": payload}
        # elasticsearch-py 7.x requires explicit Content-Type header
        # opensearch-py sets it automatically, adding it causes duplicates
        if isinstance(self.es, Elasticsearch):
            kwargs["headers"] = {"Content-Type": "application/json"}
        with translate_transport_errors():
            response = self.es.transport.perform_request("POST", path, **kwargs)
        # When method is HEAD and code is 404 perform request returns True
        # So response is Union[bool, Any]
        if isinstance(response, bool):
            raise exceptions.UnexpectedRequestResponse()
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
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)
