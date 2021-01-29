from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Any, Dict, List, Optional, Tuple  # pragma: no cover

from elasticsearch import Elasticsearch, RequestsHttpConnection
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
    context: Optional[Dict[Any, Any]] = None,
    **kwargs: Any,
):  # pragma: no cover
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
        host: str = "localhost",
        port: int = 443,
        path: str = "",
        scheme: str = "https",
        user: Optional[str] = None,
        password: Optional[str] = None,
        context: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
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
        # AWS configured credentials on the connection string
        elif (
            "aws_access_key" in kwargs
            and "aws_secret_key" in kwargs
            and "aws_region" in kwargs
        ):
            aws_auth = self._aws_auth(
                kwargs["aws_access_key"], kwargs["aws_secret_key"], kwargs["aws_region"]
            )
            kwargs.pop("aws_access_key")
            kwargs.pop("aws_secret_key")
            kwargs.pop("aws_region")

            self.es = Elasticsearch(
                self.url,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                **kwargs,
            )
        # aws_profile=<region>
        elif "aws_profile" in kwargs:
            aws_auth = self._aws_auth_profile(kwargs["aws_profile"])
            self.es = Elasticsearch(
                self.url,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                **kwargs,
            )
        else:
            self.es = Elasticsearch(self.url, **self.kwargs)

    @staticmethod
    def _aws_auth_profile(region):
        from requests_aws4auth import AWS4Auth
        import boto3

        service = "es"
        credentials = boto3.Session().get_credentials()
        return AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service,
            session_token=credentials.token,
        )

    @staticmethod
    def _aws_auth(aws_access_key: str, aws_secret_key: str, region: str) -> Any:
        from requests_aws4auth import AWS4Auth

        return AWS4Auth(aws_access_key, aws_secret_key, region, "es")

    @check_closed
    def cursor(self) -> "Cursor":
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.url, self.es, **self.kwargs)
        self.cursors.append(cursor)
        return cursor


class Cursor(BaseCursor):

    """Connection cursor."""

    def __init__(self, url, es, **kwargs):
        super().__init__(url, es, **kwargs)
        self.sql_path = kwargs.get("sql_path") or "_opendistro/_sql"

    def get_valid_table_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response
        Mixes `SHOW TABLES LIKE` with direct index access info to exclude indexes
        that have no rows so no columns (unless templated). SQLAlchemy will
        not support reflection of tables with no columns

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

    def _traverse_mapping(
        self,
        mapping: Dict[str, Any],
        results: List[Tuple[str, ...]],
        parent_field_name=None,
    ) -> List[Tuple[str, ...]]:
        for field_name, metadata in mapping.items():
            if parent_field_name:
                field_name = f"{parent_field_name}.{field_name}"
            if "properties" in metadata:
                self._traverse_mapping(metadata["properties"], results, field_name)
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
        get's all fields by directly accessing `<index>/_mapping/` endpoint

        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        response = self.es.indices.get_mapping(index=index_name, format="json")
        self._results = self._traverse_mapping(
            response[index_name]["mappings"]["properties"], []
        )

        self.description = get_description_from_columns(
            [
                {"name": "COLUMN_NAME", "type": "text"},
                {"name": "TYPE_NAME", "type": "text"},
            ]
        )
        return self

    def get_valid_select_one(self) -> "Cursor":
        res = self.es.ping()
        if not res:
            raise exceptions.DatabaseError()
        self._results = [(1,)]
        self.description = get_description_from_columns([{"name": "1", "type": "long"}])
        return self

    @check_closed
    def execute(self, operation, parameters=None) -> "Cursor":
        if operation == "SHOW VALID_TABLES":
            return self.get_valid_table_names()

        if operation.lower() == "select 1":
            return self.get_valid_select_one()

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
