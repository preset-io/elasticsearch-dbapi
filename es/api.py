from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Dict

from elasticsearch import Elasticsearch, exceptions as es_exceptions

from six import string_types
from six.moves.urllib import parse


from . import exceptions
from .const import DEFAULT_SCHEMA


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3
    DATETIME = 4


def connect(
    host="localhost",
    port=9200,
    path="",
    scheme="http",
    user=None,
    password=None,
    context=None,
    header=False,
):  # noqa: E125
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()

    """
    context = context or {}
    return Connection(host, port, path, scheme, user, password, context, header)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                "{klass} already closed".format(klass=self.__class__.__name__),
            )
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise exceptions.Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return g


def get_type(data_type):
    type_map = {
        "text": Type.STRING,
        "keyword": Type.STRING,
        "integer": Type.NUMBER,
        "half_float": Type.NUMBER,
        "geo_point": Type.STRING,
        # TODO get a solution for nested type
        "nested": Type.STRING,
        "date": Type.DATETIME,
        "datetime": Type.DATETIME,
        "long": Type.NUMBER,
        "float": Type.NUMBER,
        "double": Type.NUMBER,
        "bytes": Type.NUMBER,
        "boolean": Type.BOOLEAN,
        "ip": Type.STRING,
    }
    return type_map[data_type.lower()]


def get_description_from_columns(columns: Dict):
    return [
        (
            column.get("name"),  # name
            get_type(column.get("type")),  # type code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            True,  # [null_ok]
        )
        for column in columns
    ]


class Connection(object):

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
        header=False,
    ):
        netloc = "{host}:{port}".format(host=host, port=port)
        self.url = parse.urlunparse((scheme, netloc, path, None, None, None))
        self.es = Elasticsearch(self.url)
        self.context = context or {}
        self.closed = False
        self.cursors = []
        self.header = header
        self.user = user
        self.password = password

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
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.url, self.user, self.password, self.context, self.header)
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

    def __init__(self, url, user=None, password=None, context=None, header=False):
        self.url = url
        self.context = context or {}
        self.header = header
        self.user = user
        self.password = password
        self.es = Elasticsearch(self.url)
        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to an iterator after a successfull query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self):
        return len(self._results)

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None):
        re_table_name = re.match("SHOW ARRAY_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_array_type_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self._http_query(query)
        rows = results.get("rows")
        columns = results.get("columns")
        if rows:
            self._results = rows
            self.description = get_description_from_columns(columns)
        return self

    def get_array_type_columns(self, table_name: str) -> "Cursor":
        """
            Queries the index (table) for just one record
            and return a list of array type columns.
            This is useful since arrays are not supported by ES SQL
        """
        array_columns = []
        try:
            resp = self.es.search(table_name, size=1)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}",
            )
        except es_exceptions.NotFoundError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}",
            )
        try:
            _source = resp["hits"]["hits"][0]["_source"]
        except KeyError as e:
            raise exceptions.DataError(
                f"Error inferring array type columns {self.url}: {e}",
            )
        for col_name, value in _source.items():
            # If it's a list (ES Array add to cursor)
            if isinstance(value, list):
                if len(value) > 0:
                    # If it's an array of objects add all keys
                    if isinstance(value[0], dict):
                        for in_col_name in value[0]:
                            array_columns.append([f"{col_name}.{in_col_name}"])
                            array_columns.append([f"{col_name}.{in_col_name}.keyword"])
                        continue
                array_columns.append([col_name])
                array_columns.append([f"{col_name}.keyword"])
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = array_columns
        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead",
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
        return list(self._results)

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

    def _sanitize_query(self, query):
        query = query.replace("  ", " ")
        query = query.replace("\n", " ")
        # remove dummy schema from queries
        return query.replace(f'FROM "{DEFAULT_SCHEMA}".', "FROM ")

    def _http_query(self, query: str):
        """
        Request an http SQL query to elasticsearch
        """
        self.description = None
        # Sanitize query
        query = self._sanitize_query(query)
        payload = {"query": query}
        try:
            resp = self.es.transport.perform_request("POST", "/_sql/", body=payload)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}",
            )
        except es_exceptions.RequestError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}",
            )
        return resp


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
