from elasticsearch import exceptions as es_exceptions
from es import exceptions
from six import string_types
from six.moves.urllib import parse


from .const import DEFAULT_FETCH_SIZE, DEFAULT_SCHEMA, DEFAULT_SQL_PATH


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3
    DATETIME = 4


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                "{klass} already closed".format(klass=self.__class__.__name__)
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
        raise NotImplementedError  # pragma: no cover

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

    def elastic_query(self, query: str, csv=False):
        """
        Request an http SQL query to elasticsearch
        """
        self.description = None
        # Sanitize query
        query = self.sanitize_query(query)
        payload = {"query": query, "fetch_size": self.fetch_size}
        if csv:
            path = f"/{self.sql_path}/?format=csv"
        else:
            path = f"/{self.sql_path}/"
        try:
            resp = self.es.transport.perform_request("POST", path, body=payload)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}"
            )
        except es_exceptions.RequestError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}"
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
