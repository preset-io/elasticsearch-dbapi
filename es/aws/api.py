from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import csv
import re

from elasticsearch import Elasticsearch
from es import exceptions
from es.baseapi import apply_parameters, BaseConnection, BaseCursor, check_closed, Type
from es.const import DEFAULT_SCHEMA


def connect(
    host="localhost",
    port=443,
    path="",
    scheme="https",
    user=None,
    password=None,
    context=None,
    **kwargs,
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
        results = self.elastic_query("SHOW TABLES LIKE *")
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = [[result] for result in results]
        return self

    def _show_columns(self, table_name):
        """
            Simulates SHOW COLUMNS FROM more like SQL from elastic itself
        """
        results = self.elastic_query(f"SHOW TABLES LIKE {table_name}")
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

    @check_closed
    def execute(self, operation, parameters=None):
        if operation == "SHOW TABLES":
            return self._show_tables()
        re_table_name = re.match("SHOW COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self._show_columns(re_table_name[1])

        re_table_name = re.match("SHOW ARRAY_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_array_type_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        _results = self.elastic_query(query, csv=True).split("\n")
        header = _results[0].split(",")
        _results = _results[1:]
        results = list(csv.reader(_results))
        self.description = get_description_from_first_row(header, results[0])
        self._results = results
        return self

    def get_array_type_columns(self, table_name: str) -> "Cursor":
        """
            Queries the index (table) for just one record
            and return a list of array type columns.
            This is useful since arrays are not supported by ES SQL
        """
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = [[]]
        return self

    def sanitize_query(self, query):
        query = query.replace('"', "")
        query = query.replace("  ", " ")
        query = query.replace("\n", " ")
        # remove dummy schema from queries
        return query.replace(f"FROM {DEFAULT_SCHEMA}.", "FROM ")
