from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from typing import Any, Dict, Optional  # pragma: no cover

from elasticsearch import Elasticsearch
from es import exceptions
from es.baseapi import apply_parameters, BaseConnection, BaseCursor, check_closed, Type
from es.const import DEFAULT_SCHEMA


def connect(
    host: str = "localhost",
    port: int = 443,
    path: str = "",
    scheme: str = "https",
    user: Optional[str] = None,
    password: Optional[str] = None,
    context: Optional[Dict] = None,
    **kwargs: Any,
):  # pragma: no cover
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()

    """
    context = context or {}
    return Connection(host, port, path, scheme, user, password, context, **kwargs)


def get_type_from_value(value):  # pragma: no cover
    if value in ("true", "false"):
        return Type.BOOLEAN
    try:
        float(value)
        return Type.NUMBER
    except ValueError:
        return Type.STRING


def get_description_from_first_row(header: list, row: list):  # pragma: no cover
    description = []
    for i, col_name in enumerate(header):
        description.append(
            (
                col_name,
                get_type_from_value(row[i]),
                None,  # [display_size]
                None,  # [internal_size]
                None,  # [precision]
                None,  # [scale]
                True,  # [null_ok]
            )
        )
    return description


class Connection(BaseConnection):  # pragma: no cover

    """Connection to an ES Cluster """

    def __init__(
        self,
        host="localhost",
        port=443,
        path="",
        scheme="https",
        user=None,
        password=None,
        context=None,
        **kwargs,
    ):
        super().__init__(
            host=host,
            port=port,
            path=path,
            scheme=scheme,
            user=user,
            password=password,
            context=context,
            **kwargs,
        )
        if user and password:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **self.kwargs)
        else:
            self.es = Elasticsearch(self.url, **self.kwargs)

    def _aws_auth(self, aws_access_key, aws_secret_key, region):
        from requests_4auth import AWS4Auth

        return AWS4Auth(aws_access_key, aws_secret_key, region, "es")

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.url, self.es, **self.kwargs)
        self.cursors.append(cursor)
        return cursor


class Cursor(BaseCursor):  # pragma: no cover

    """Connection cursor."""

    def __init__(self, url, es, **kwargs):
        super().__init__(url, es, **kwargs)
        self.sql_path = kwargs.get("sql_path") or "_opendistro/_sql"

    def _show_tables(self):
        """
            Simulates SHOW TABLES more like SQL from elastic itself
        """
        results = self.elastic_query("SHOW TABLES LIKE '%'")
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = [[result] for result in results]
        return self

    def _show_columns(self, table_name):
        """
            Simulates SHOW COLUMNS FROM more like SQL from elastic itself
        """
        results = self.execute(f"DESCRIBE TABLES LIKE {table_name}")
        if table_name not in results:
            raise exceptions.ProgrammingError(f"Table {table_name} not found")
        rows = []
        for col, value in results[table_name]["mappings"]["_doc"]["properties"].items():
            type = value.get("type")
            if type:
                rows.append([col, type])
        self.description = [
            ("column", Type.STRING, None, None, None, None, None),
            ("mapping", Type.STRING, None, None, None, None, None),
        ]
        self._results = rows
        return self

    def get_valid_table_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response

        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        results = self.execute("SHOW TABLES LIKE %")
        response = self.es.cat.indices(format="json")

        _results = []
        for result in results:
            is_empty = False
            for item in response:
                if item["index"] == result[2]:
                    if int(item["docs.count"]) == 0:
                        is_empty = True
                        break
            if not is_empty:
                _results.append(result)
        self._results = _results
        return self

    @check_closed
    def execute(self, operation, parameters=None):
        from es.elastic.api import get_description_from_columns

        if operation == "SHOW VALID_TABLES":
            return self.get_valid_table_names()

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)

        rows = [tuple(row) for row in results.get("datarows")]
        columns = results.get("schema")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an opendistro sql ep"
            )
        self._results = rows
        self.description = get_description_from_columns(columns)
        return self

    def sanitize_query(self, query):
        query = query.replace('"', "")
        query = query.replace("  ", " ")
        query = query.replace("\n", " ")
        # remove dummy schema from queries
        return query.replace(f"FROM {DEFAULT_SCHEMA}.", "FROM ")
