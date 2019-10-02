from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Dict

from elasticsearch import Elasticsearch, exceptions as es_exceptions

from six import string_types

from es import exceptions
from es.baseapi import Type, check_closed, BaseConnection, BaseCursor, apply_parameters
from es.const import DEFAULT_SCHEMA, DEFAULT_SQL_PATH


def connect(
        host="localhost",
        port=443,
        path="",
        scheme="https",
        user=None,
        password=None,
        context=None,
        **kwargs,
):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()

    """
    context = context or {}
    return Connection(
        host, port, path, scheme, user, password, context, **kwargs,
    )


def get_type(data):
    if isinstance(data, str):
        return Type.STRING
    if isinstance(data, int):
        return Type.NUMBER
    if isinstance(data, bool):
        return Type.BOOLEAN


def get_description_from_first_doc(doc: Dict):
    return [
        (
            col_name,  # name
            get_type(value),  # type code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            True,  # [null_ok]
        )
        for col_name, value in doc.items()
    ]


class Connection(BaseConnection):

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
            self.es = Elasticsearch(
                self.url,
                user=user,
                password=password,
            )
        else:
            self.es = Elasticsearch(self.url)

    def _aws_auth(self, aws_access_key, aws_secret_key, region):
        from requests_4auth import AWS4Auth
        return AWS4Auth(aws_access_key, aws_secret_key, region, 'es')

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.url, self.es, **self.kwargs)
        self.cursors.append(cursor)
        return cursor


class Cursor(BaseCursor):

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
        for result in results:
            rows.append(result)
        self.description = [
            ("name", Type.STRING, None, None, None, None, None),
            ("type", Type.STRING, None, None, None, None, None),
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
        results = self.elastic_query(query)
        if len(results["hits"]["hits"]) == 0:
            return self
        first_row = results["hits"]["hits"][0]["_source"]
        self.description = get_description_from_first_doc(first_row)
        rows = []
        for result in results["hits"]["hits"]:
            row = []
            for key, value in result["_source"].items():
                row.append(value)
            rows.append(row)
        self._results = rows
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
