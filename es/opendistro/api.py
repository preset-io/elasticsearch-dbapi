from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import ConnectionError
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
) -> BaseConnection:
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
        if user and password and "aws_keys" not in kwargs:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **self.kwargs)
        # AWS configured credentials on the connection string
        elif user and password and "aws_keys" in kwargs and "aws_region" in kwargs:
            aws_auth = self._aws_auth(user, password, kwargs["aws_region"])
            kwargs.pop("aws_keys")
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
    def _aws_auth_profile(region: str) -> Any:
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

    custom_sql_to_method = {
        "show valid_tables": "get_valid_table_names",
        "show valid_views": "get_valid_view_names",
        "select 1": "get_valid_select_one",
    }

    def __init__(self, url: str, es: Elasticsearch, **kwargs: Any) -> None:
        super().__init__(url, es, **kwargs)
        self.sql_path = kwargs.get("sql_path") or "_opendistro/_sql"
        # Opendistro SQL v2 flag
        self.v2 = kwargs.get("v2", False)
        if self.v2:
            self.fetch_size = None

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

    def get_valid_view_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_VIEWS" excludes empty indices from the response
        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        if self.v2:
            # On v2 an alias is represented has a table
            return self
        response = self.es.cat.aliases(format="json")
        results: List[Tuple[str, ...]] = []
        for item in response:
            results.append((item["alias"], item["index"]))
        self.description = get_description_from_columns(
            [
                {"name": "VIEW_NAME", "type": "text"},
                {"name": "TABLE_NAME", "type": "text"},
            ]
        )
        self._results = results
        return self

    def _traverse_mapping(
        self,
        mapping: Dict[str, Any],
        results: List[Tuple[str, ...]],
        parent_field_name: Optional[str] = None,
    ) -> List[Tuple[str, ...]]:
        """
        Traverses an Elasticsearch mapping and returns a flattened list
        of fields and types. Nested fields are flattened using dotted notation

        :param mapping: An elastic search mapping
        :param results: A list of fields and types
        :param parent_field_name: recursively append
        child field names to parent field names
        :return: A flattened list of fields and types
        """
        for field_name, metadata in mapping.items():
            if parent_field_name:
                field_name = f"{parent_field_name}.{field_name}"
            if "properties" in metadata:
                self._traverse_mapping(metadata["properties"], results, field_name)
            else:
                results.append((field_name, metadata["type"]))
            if "fields" in metadata:
                for sub_field_name, sub_metadata in metadata["fields"].items():
                    # V2 does not recognize keyword fields
                    if sub_field_name.endswith("keyword") and self.v2:
                        continue
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
        # When the index is an alias the first key is the real index name
        try:
            index_real_name = list(response.keys())[0]
        except IndexError:
            raise exceptions.DataError("Index mapping returned and unexpected response")
        self._results = self._traverse_mapping(
            response[index_real_name]["mappings"]["properties"], []
        )

        self.description = get_description_from_columns(
            [
                {"name": "COLUMN_NAME", "type": "text"},
                {"name": "TYPE_NAME", "type": "text"},
            ]
        )
        return self

    def get_valid_select_one(self) -> "Cursor":
        """
        Currently Opendistro SQL endpoint does not support SELECT 1
        So we use Elasticsearch ping method

        :return: A cursor with "1" (result from SELECT 1)
        :raises: DatabaseError in case of a connection error
        """
        try:
            res = self.es.ping()
        except ConnectionError:
            raise exceptions.DatabaseError("Connection failed")
        if not res:
            raise exceptions.DatabaseError("Connection failed")
        self._results = [(1,)]
        self.description = get_description_from_columns([{"name": "1", "type": "long"}])
        return self

    @check_closed
    def execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "BaseCursor":
        cursor = self.custom_sql_to_method_dispatcher(operation)
        if cursor:
            return cursor

        re_table_name = re.match("SHOW VALID_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_valid_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)

        rows = [tuple(row) for row in results.get("datarows", [])]
        columns = results.get("schema")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an elastic sql ep"
            )
        self._results = rows
        self.description = get_description_from_columns(columns)
        return self

    def sanitize_query(self, query: str) -> str:
        query = query.replace('"', "")
        query = query.replace("  ", " ")
        query = query.replace("\n", " ")
        # remove dummy schema from queries
        return query.replace(f"FROM {DEFAULT_SCHEMA}.", "FROM ")
