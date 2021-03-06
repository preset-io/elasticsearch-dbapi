from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Any, Dict, Optional

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from es import exceptions
from es.baseapi import (
    apply_parameters,
    BaseConnection,
    BaseCursor,
    check_closed,
    get_description_from_columns,
    Type,
)


def connect(
    host: str = "localhost",
    port: int = 9200,
    path: str = "",
    scheme: str = "http",
    user: Optional[str] = None,
    password: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
):
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()

    """
    context = context or {}
    return Connection(host, port, path, scheme, user, password, context, **kwargs)


class Connection(BaseConnection):

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
        self.sql_path = kwargs.get("sql_path") or "_sql"

    def get_valid_table_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response
        Mixes `SHOW TABLES` with direct index access info to exclude indexes
        that have no rows so no columns (unless templated). SQLAlchemy will
        not support reflection of tables with no columns

        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        results = self.execute("SHOW TABLES")
        response = self.es.cat.indices(format="json")

        _results = []
        for result in results:
            is_empty = False
            for item in response:
                # First column is TABLE_NAME
                if item["index"] == result[0]:
                    if int(item["docs.count"]) == 0:
                        is_empty = True
                        break
            if not is_empty:
                _results.append(result)
        self._results = _results
        return self

    @check_closed
    def execute(self, operation, parameters=None):
        if operation == "SHOW VALID_TABLES":
            return self.get_valid_table_names()

        re_table_name = re.match("SHOW ARRAY_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_array_type_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)
        # We need a list of tuples
        rows = [tuple(row) for row in results.get("rows")]
        columns = results.get("columns")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an opendistro sql ep"
            )
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
            response = self.es.search(index=table_name, size=1)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}"
            )
        except es_exceptions.NotFoundError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}"
            )
        try:
            if response["hits"]["total"]["value"] == 0:
                source = {}
            else:
                source = response["hits"]["hits"][0]["_source"]
        except KeyError as e:
            raise exceptions.DataError(
                f"Error inferring array type columns {self.url}: {e}"
            )
        for col_name, value in source.items():
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
        # Not array column found
        if not array_columns:
            array_columns = [[]]
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = array_columns
        return self
