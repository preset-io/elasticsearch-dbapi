import logging
import re
from typing import Any, cast, Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from es import exceptions
from es.baseapi import (
    apply_parameters,
    BaseConnection,
    BaseCursor,
    check_closed,
    CursorDescriptionRow,
    get_description_from_columns,
    Type,
)
from packaging import version

logger = logging.getLogger(__name__)


def connect(
    host: str = "localhost",
    port: int = 9200,
    path: str = "",
    scheme: str = "http",
    user: Optional[str] = None,
    password: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
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
    """Connection to an ES Cluster"""

    es: Optional[Elasticsearch]

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        path: str = "",
        scheme: str = "http",
        user: Optional[str] = None,
        password: Optional[str] = None,
        context: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
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
        # Filter out cursor-specific params that Elasticsearch doesn't understand
        es_kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k not in ("sql_path", "fetch_size", "time_zone", "v2")
        }
        if user and password:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **es_kwargs)
        else:
            self.es = Elasticsearch(self.url, **es_kwargs)

    @check_closed
    def cursor(self) -> BaseCursor:
        """Return a new Cursor Object using the connection."""
        if self.es:
            cursor = Cursor(self.url, self.es, **self.kwargs)
            self.cursors.append(cursor)
            return cursor
        raise exceptions.UnexpectedESInitError()


class Cursor(BaseCursor):
    """Connection cursor."""

    custom_sql_to_method = {
        "show valid_tables": "get_valid_table_names",
        "show valid_views": "get_valid_view_names",
    }

    def __init__(self, url: str, es: Elasticsearch, **kwargs: Any) -> None:
        super().__init__(url, es, **kwargs)
        self.sql_path = kwargs.get("sql_path") or "_sql"

    def _get_value_for_col_name(self, row: Tuple[Any], name: str) -> Any:
        """
        Get the value of a specific column name from a row
        :param row: The result row
        :param name: The column name
        :return: Value
        """
        for idx, col_description in enumerate(self.description):
            if col_description.name == name:
                return row[idx]

    def get_valid_table_view_names(self, type_filter: str) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response
        Mixes `SHOW TABLES` with direct index access info to exclude indexes
        that have no rows so no columns (unless templated). SQLAlchemy will
        not support reflection of tables with no columns

        https://github.com/preset-io/elasticsearch-dbapi/issues/38

        :param: type_filter will filter SHOW_TABLES result by BASE_TABLE or VIEW
        """
        results = self.execute("SHOW TABLES")
        indices_response = self.es.cat.indices(format="json")
        # Cast response to list of dicts for type checking
        indices: List[Dict[str, Any]] = cast(
            List[Dict[str, Any]], list(indices_response)
        )

        _results = []
        for result in results:
            is_empty = False
            for item in indices:
                # First column is TABLE_NAME
                if item["index"] == self._get_value_for_col_name(result, "name"):
                    if int(item["docs.count"]) == 0:
                        is_empty = True
                        break
            if (
                not is_empty
                and self._get_value_for_col_name(result, "type") == type_filter
            ):
                _results.append(result)
        self._results = _results
        return self

    def get_valid_table_names(self) -> "Cursor":
        # Get the ES cluster version. Since 7.10 the table column name changed #52
        cluster_info = self.es.info()
        cluster_version = version.parse(cluster_info["version"]["number"])
        if cluster_version >= version.parse("7.10.0"):
            return self.get_valid_table_view_names("TABLE")
        return self.get_valid_table_view_names("BASE TABLE")

    def get_valid_view_names(self) -> "Cursor":
        return self.get_valid_table_view_names("VIEW")

    @check_closed
    def execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "BaseCursor":
        cursor = self.custom_sql_to_method_dispatcher(operation)
        if cursor:
            return cursor

        re_table_name = re.match("SHOW ARRAY_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_array_type_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)
        columns = results.get("columns")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an opendistro sql ep"
            )
        self.description = get_description_from_columns(columns)

        # We need a list of tuples
        rows = [tuple(row) for row in results.get("rows", [])]

        # Elasticsearch's SQL API only returns up to `fetch_size` rows per
        # request. A larger result set comes back with a `cursor` that must
        # be followed to fetch subsequent pages, or later rows are silently
        # dropped. See:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-pagination.html
        es_cursor = results.get("cursor")
        try:
            while es_cursor:
                results = self.elastic_cursor_query(es_cursor)
                rows.extend(tuple(row) for row in results.get("rows", []))
                es_cursor = results.get("cursor")
        finally:
            # A cursor is only still open here if we're exiting via an
            # exception raised mid-pagination; ES returns no cursor on the
            # final page, so on a normal, fully-consumed loop there's
            # nothing left to close server-side.
            if es_cursor:
                self.close_elastic_cursor(es_cursor)

        self._results = rows
        return self

    def elastic_cursor_query(self, cursor: str) -> Dict[str, Any]:
        """
        Follow an Elasticsearch SQL cursor to fetch the next page of a
        result set.
        """
        path = f"/{self.sql_path}/"
        kwargs: Dict[str, Any] = {"body": {"cursor": cursor}}
        if isinstance(self.es, Elasticsearch):
            kwargs["headers"] = {"Content-Type": "application/json"}
        try:
            response = self.es.transport.perform_request("POST", path, **kwargs)
        except es_exceptions.ConnectionError:
            raise exceptions.OperationalError(
                "Error connecting to Elasticsearch/OpenSearch"
            )
        except es_exceptions.RequestError as ex:
            raise exceptions.ProgrammingError(f"Error ({ex.error}): {ex.info}")
        if isinstance(response, bool):
            raise exceptions.UnexpectedRequestResponse()
        if "error" in response:
            raise exceptions.ProgrammingError(
                f"({response['error']['reason']}): {response['error']['details']}"
            )
        return response

    def close_elastic_cursor(self, cursor: str) -> None:
        """
        Release server-side resources held by an open Elasticsearch SQL
        cursor. Best-effort: a failure here is logged, not raised, so it
        doesn't mask the original query result or error.
        """
        path = f"/{self.sql_path}/close"
        kwargs: Dict[str, Any] = {"body": {"cursor": cursor}}
        if isinstance(self.es, Elasticsearch):
            kwargs["headers"] = {"Content-Type": "application/json"}
        try:
            self.es.transport.perform_request("POST", path, **kwargs)
        except Exception:
            logger.warning("Failed to close Elasticsearch SQL cursor", exc_info=True)

    def get_array_type_columns(self, table_name: str) -> "Cursor":
        """
        Queries the index (table) for just one record
        and return a list of array type columns.
        This is useful since arrays are not supported by ES SQL
        """
        array_columns: List[Tuple[Any, ...]] = []
        try:
            response = self.es.search(index=table_name, size=1)
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(
                f"Error connecting to {self.url}: {e.info}"
            )
        except es_exceptions.NotFoundError as e:
            raise exceptions.ProgrammingError(f"Error ({e.error}): {e.info}")
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
                            array_columns.append((f"{col_name}.{in_col_name}",))
                            array_columns.append((f"{col_name}.{in_col_name}.keyword",))
                        continue
                array_columns.append((col_name,))
                array_columns.append((f"{col_name}.keyword",))
        if not array_columns:
            array_columns = []
        self.description = [
            CursorDescriptionRow("name", Type.STRING, None, None, None, None, None)
        ]
        self._results = array_columns
        return self
