from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Dict

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from es import exceptions
from es.baseapi import apply_parameters, BaseConnection, BaseCursor, check_closed, Type


def connect(
    host="localhost",
    port=9200,
    path="",
    scheme="http",
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
    return Connection(host, port, path, scheme, user, password, context, **kwargs)


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

    @check_closed
    def execute(self, operation, parameters=None):
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
            resp = self.es.search(index=table_name, size=1)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}"
            )
        except es_exceptions.NotFoundError as e:
            raise exceptions.ProgrammingError(
                f"Error ({e.error}): {e.info['error']['reason']}"
            )
        try:
            _source = resp["hits"]["hits"][0]["_source"]
        except KeyError as e:
            raise exceptions.DataError(
                f"Error inferring array type columns {self.url}: {e}"
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
        # Not array column found
        if not array_columns:
            array_columns = [[]]
        self.description = [("name", Type.STRING, None, None, None, None, None)]
        self._results = array_columns
        return self
