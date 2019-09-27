from collections import namedtuple, OrderedDict
from enum import Enum
from pprint import pformat

# TODO Remove six imports
from six import string_types
from six.moves.urllib import parse

import requests

from . import exceptions
import logging

logger = logging.getLogger(__name__)


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(*args, **kwargs):
    """
    Constructor for creating a connection to the database.
        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()
    """
    return Connection(*args, **kwargs)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(f"{self.__class__.__name__} already closed")
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return g


def get_description_from_types(column_names, types):
    return [
        (
            name,  # name
            type_code,  # type_code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            None,  # [null_ok]
        )
        for name, type_code in zip(column_names, types)
    ]


def get_types_from_rows(column_names, rows):
    """
    Return description by scraping the rows
    We only return the name and type (inferred from the data).
    """
    if not column_names:
        return []
    if not rows:
        raise exceptions.InternalError(f"Cannot infer the column types from empty rows")
    types = [None] * len(column_names)
    remaining = len(column_names)
    TypeCodeAndValue = namedtuple("TypeCodeAndValue", ["code", "value"])
    for row in rows:
        if remaining <= 0:
            break
        if len(row) != len(column_names):
            raise exceptions.DatabaseError(
                f"Column names {column_names} does not match row {row}"
            )
        for column_index, value in enumerate(row):
            if value is not None:
                current_type = types[column_index]
                new_type = get_type(value)
                if current_type is None:
                    types[column_index] = TypeCodeAndValue(value=value, code=new_type)
                    remaining -= 1
                elif new_type is not current_type.code:
                    raise exceptions.DatabaseError(
                        f"Differing column type found for column {name}:"
                        f"{current_type} vs {TypeCodeAndValue(code=new_type, value=value)}"
                    )
    if any([t is None for t in types]):
        raise exceptions.DatabaseError(f"Couldn't infer all the types {types}")
    return [t.code for t in types]


def get_type(value):
    """Infer type from value."""
    if isinstance(value, string_types):
        return Type.STRING
    elif isinstance(value, (int, float)):
        return Type.NUMBER
    elif isinstance(value, bool):
        return Type.BOOLEAN

    raise exceptions.Error(f"Value of unknown type: {value}")


class Connection(object):

    """Connection to a Elasticsearch database."""

    def __init__(self, *args, **kwargs):
        self._debug = kwargs.get("debug", False)
        self._args = args
        self._kwargs = kwargs
        self.closed = False
        self.cursors = []

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
        Commit any pending transaction to the database.
        Not supported.
        """
        raise NotImplemented

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(*self._args, **self._kwargs)
        self.cursors.append(cursor)
        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):
    """Connection cursor."""

    def __init__(
        self,
        host,
        port=9200,
        scheme="http",
        path="/_sql?format=json",
        extra_request_headers="",
        debug=False,
    ):
        self.url = parse.urlunparse((scheme, f"{host}:{port}", path, None, None, None))

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # these are updated only after a query
        self.description = None
        self.rowcount = -1
        self._results = None
        self._debug = debug
        extra_headers = {}
        if extra_request_headers:
            for header in extra_request_headers.split(","):
                k, v = header.split("=")
                extra_headers[k] = v
        self._extra_request_headers = extra_headers

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters or {})

        headers = {"Content-Type": "application/json"}
        headers.update(self._extra_request_headers)
        payload = {"query": query}
        if self._debug:
            logger.info(
                f"Submitting the ES query to {self.url}:\n{query}\n{pformat(payload)}, with {headers}"
            )
        r = requests.post(self.url, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = "utf-8"

        try:
            payload = r.json()
        except Exception as e:
            raise exceptions.DatabaseError(
                f"Error when querying {query} from {self.url}, raw response is:\n{r.text}"
            ) from e

        # raise any error messages
        if r.status_code != 200:
            msg = f"Query:{query} returned an error: {r.status_code} {pformat(payload)}"
            raise exceptions.ProgrammingError(msg)

        rows = payload.get("rows")
        column_names = payload.get("columns")
        self.description = None
        self._results = []
        if rows:
            types = get_types_from_rows(column_names, rows)
            self._results = rows
            self.description = get_description_from_types(column_names, types)

        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead"
        )

    @check_result
    @check_closed
    def fetchone(self):
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
    def fetchmany(self, size=None):
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
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self)

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
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


def apply_parameters(operation, parameters):
    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def escape(value):
    if value == "*":
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)
