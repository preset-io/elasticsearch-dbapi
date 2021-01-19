from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Any, Dict, List, Optional, Tuple  # pragma: no cover

from elasticsearch import Elasticsearch
from es import exceptions
from es.baseapi import (
    apply_parameters,
    BaseConnection,
    BaseCursor,
    check_closed,
    get_description_from_columns,
)
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
                # Third column is TABLE_NAME
                if item["index"] == result[2]:
                    if int(item["docs.count"]) == 0:
                        is_empty = True
                        break
            if not is_empty:
                _results.append(result)
        self._results = _results
        return self

    def _tranverse_mapping(
        self, mapping: Dict[str, Any], results: List[Tuple[str]], parent_field_name=None
    ):
        for field_name, metadata in mapping.items():
            if parent_field_name:
                field_name = f"{parent_field_name}.{field_name}"
            if "properties" in metadata:
                self._tranverse_mapping(metadata["properties"], results, field_name)
            else:
                results.append((field_name, metadata["type"]))
            if "fields" in metadata:
                for sub_field_name, sub_metadata in metadata["fields"].items():
                    results.append(
                        (f"{field_name}.{sub_field_name}", sub_metadata["type"])
                    )
        return results

    def get_valid_columns(self, index_name: str) -> "Cursor":
        """
        Custom for "SHOW VALID_COLUMNS FROM <INDEX>"
        Adds keywords to text if they exist and flattens nested structures

        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        response = self.es.indices.get_mapping(index=index_name, format="json")
        self._results = self._tranverse_mapping(
            response[index_name]["mappings"]["properties"], []
        )

        self.description = get_description_from_columns(
            [
                {"name": "COLUMN_NAME", "type": "text"},
                {"name": "TYPE_NAME", "type": "text"},
            ]
        )
        return self

    @check_closed
    def execute(self, operation, parameters=None):
        if operation == "SHOW VALID_TABLES":
            return self.get_valid_table_names()

        re_table_name = re.match("SHOW VALID_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_valid_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)

        rows = [tuple(row) for row in results.get("datarows")]
        columns = results.get("schema")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an elastic sql ep"
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
