from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import deque
import logging
import re
from typing import Any, cast, Dict, List, Optional, Tuple

from es import exceptions
from es.baseapi import (
    _AUTH_ERRORS,
    apply_parameters,
    BaseConnection,
    BaseCursor,
    check_closed,
    convert_rows,
    get_description_from_columns,
    parse_bool_argument,
    translate_transport_errors,
)
from es.const import DEFAULT_SCHEMA
from es.opendistro.sqltext import (
    grouped_order_probe,
    has_subquery,
    limit_subqueries,
    outer_clauses,
    rename_colliding_aliases,
)
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import ConnectionError, TransportError

logger = logging.getLogger(__name__)

LEGACY_SQL_PATH = "_opendistro/_sql"

# The legacy engine's aggregations return at most this many buckets.
LEGACY_BUCKET_LIMIT = 200
# Unpaged SQL aggregations can stop here without a cursor.
UNPAGED_BUCKET_LIMIT = 1000
# Open Distro's default opendistro.query.size_limit.
OPEN_DISTRO_DEFAULT_SIZE_LIMIT = 200
# Elasticsearch's default index.max_result_window: the most rows one search
# (and so one SQL query without a cursor) can return.
MAX_RESULT_WINDOW = 10000

# time_zone values meaning UTC, the only zone the SQL plugin returns
_UTC_TIME_ZONE_RE = re.compile(
    r"(?:ETC/)?(?:UTC|GMT|UCT|Z|ZULU|UNIVERSAL)?(?:[+-]?0{1,2}(?::?00)?)?"
)

# A statement the SQL plugin can page with a cursor contains none of these.
# With ``fetch_size`` OpenSearch hands the others to its legacy engine, which
# silently caps aggregations and DISTINCT at 200 buckets. Unpaged v2
# aggregations have a separate 1000-bucket ceiling.
_AGGREGATION_RE = re.compile(
    r"\b(?:GROUP\s+BY|HAVING|DISTINCT)\b"
    r"|\b(?:COUNT|SUM|AVG|MIN|MAX|STD\w*|VAR\w*|PERCENTILE\w*|MEDIAN|TOPHITS"
    r"|TAKE|FIRST|LAST|APPROX\w*)\s*\(",
    re.IGNORECASE,
)
_UNPAGEABLE_RE = re.compile(
    _AGGREGATION_RE.pattern + r"|\b(?:JOIN|UNION|INTERSECT|EXCEPT|MINUS|OVER)\b",
    re.IGNORECASE,
)
# string literals, quoted identifiers and comments
_QUOTED_RE = re.compile(r"'(?:[^']|'')*'|`[^`]*`|\"[^\"]*\"|--[^\n]*|/\*.*?\*/", re.S)
_TRAILING_LIMIT_RE = re.compile(
    r"\bLIMIT\s+(\d+)(?:\s+OFFSET\s+\d+)?\s*;?\s*$", re.IGNORECASE
)


def is_pageable(query: str) -> bool:
    """
    Whether the SQL plugin can page ``query`` with a cursor: a single plain
    SELECT, without aggregation, grouping, DISTINCT, joins, set operations,
    window functions or subqueries. Literals, quoted identifiers and comments
    are ignored. When unsure the answer is no: the statement is then sent
    unpaged and its result checked for truncation instead.
    """
    bare = _QUOTED_RE.sub(" ", query)
    if not re.match(r"\s*SELECT\b", bare, re.IGNORECASE):
        return False
    if len(re.findall(r"\bSELECT\b", bare, re.IGNORECASE)) != 1:
        return False
    return not _UNPAGEABLE_RE.search(bare)


def _is_result_window_error(error: Exception) -> bool:
    """Whether the cluster refused a query for asking past one search window."""
    return "Result window is too large" in str(error)


def is_utc_time_zone(time_zone: Any) -> bool:
    normalized = str(time_zone).strip().upper()
    return bool(normalized) and bool(_UTC_TIME_ZONE_RE.fullmatch(normalized))


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


# Elasticsearch 7.10 errors for the "index" at the SQL endpoint's first segment
_MISSING_ENDPOINT_ERRORS = ("invalid_index_name_exception", "index_not_found_exception")


def _error_details(error: exceptions.DatabaseError) -> Dict[str, Any]:
    """The ``error`` object of the response behind a translated error."""
    cause = error.__cause__
    info = cause.info if isinstance(cause, TransportError) else None
    details = info.get("error") if isinstance(info, dict) else None
    return details if isinstance(details, dict) else {}


class Connection(BaseConnection):
    """Connection to an ES Cluster"""

    es: Optional[OpenSearch]

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
        time_zone = kwargs.pop("time_zone", None)
        if time_zone is not None and not is_utc_time_zone(time_zone):
            # The SQL plugin accepts the parameter and ignores it, returning
            # UTC. It is dropped rather than refused, so an existing
            # connection string keeps connecting.
            logger.warning(
                "time_zone=%s is ignored: the OpenSearch/OpenDistro SQL plugin "
                "always returns datetime values in UTC",
                time_zone,
            )
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
        # Filter out cursor-specific params that OpenSearch doesn't understand
        os_kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k not in ("sql_path", "fetch_size", "time_zone", "v2")
        }
        if user and password and "aws_keys" not in kwargs:
            self.es = OpenSearch(self.url, http_auth=(user, password), **os_kwargs)
        # AWS configured credentials on the connection string
        elif user and password and "aws_keys" in kwargs and "aws_region" in kwargs:
            aws_auth = self._aws_auth(user, password, kwargs["aws_region"])
            os_kwargs.pop("aws_keys", None)
            os_kwargs.pop("aws_region", None)

            self.es = OpenSearch(
                self.url,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                **os_kwargs,
            )
        # aws_profile=<region>
        elif "aws_profile" in kwargs:
            aws_auth = self._aws_auth_profile(kwargs["aws_profile"])
            os_kwargs.pop("aws_profile", None)
            self.es = OpenSearch(
                self.url,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                **os_kwargs,
            )
        else:
            self.es = OpenSearch(self.url, **os_kwargs)

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
        if self.es:
            # the cursor records the detected SQL endpoint in self.kwargs
            cursor = Cursor(
                self.url,
                self.es,
                _connection_state=self.kwargs,
                **self.kwargs,
            )
            self.cursors.append(cursor)
            return cursor
        raise exceptions.UnexpectedESInitError()


class Cursor(BaseCursor):

    custom_sql_to_method = {
        "show valid_tables": "get_valid_table_names",
        "show valid_views": "get_valid_view_names",
        "select 1": "get_valid_select_one",
    }

    def __init__(self, url: str, es: OpenSearch, **kwargs: Any) -> None:
        super().__init__(url, es, **kwargs)
        # OpenSearch serves SQL on _plugins/_sql (1.x onwards) and removed the
        # legacy _opendistro/_sql endpoint in 3.0, while Open Distro for
        # Elasticsearch only has the legacy one. Without an explicit sql_path
        # the modern endpoint is used, falling back once to the legacy one.
        self._sql_path_explicit = bool(kwargs.get("sql_path"))
        self.sql_path = kwargs.get("sql_path") or kwargs.get(
            "_detected_sql_path", "_plugins/_sql"
        )
        self._connection_kwargs: Dict[str, Any] = kwargs.get("_connection_state", {})
        # Opendistro SQL v2 flag. From a connection URL it arrives as a string,
        # and "False" must not switch v2 on.
        self.v2 = self._parse_v2(kwargs.get("v2", False))
        self._last_paged = False
        # In v2 mode fetch_size is sent only for statements the plugin can page
        # (see ``is_pageable``): without it a plain SELECT stops at
        # plugins.query.size_limit rows with no cursor, while with it
        # aggregations run on the legacy engine, capped at 200 buckets.

    @staticmethod
    def _parse_v2(value: Any) -> bool:
        if not isinstance(value, str):
            return bool(value)
        try:
            return parse_bool_argument(value)
        except ValueError:
            logger.warning("Unrecognised v2=%s, treated as true", value)
            return bool(value)

    def get_valid_table_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response
        Mixes `SHOW TABLES LIKE` with direct index access info to exclude indexes
        that have no rows so no columns (unless templated). SQLAlchemy will
        not support reflection of tables with no columns

        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        results = self.execute("SHOW TABLES LIKE %")
        empty = self.empty_index_names()
        # Third column is TABLE_NAME
        self._results = deque([result for result in results if result[2] not in empty])
        return self

    def get_valid_view_names(self) -> "Cursor":
        """
        Custom for "SHOW VALID_VIEWS" excludes empty indices from the response
        https://github.com/preset-io/elasticsearch-dbapi/issues/38
        """
        # v2 engines list aliases among tables on OpenSearch 2.x but not on
        # 3.x; list as views only the aliases that are not already tables.
        as_tables = (
            {row[2] for row in self.execute("SHOW TABLES LIKE %")} if self.v2 else set()
        )
        try:
            aliases_response = self.es.cat.aliases(format="json")
        except _AUTH_ERRORS as ex:
            # Listing aliases needs indices:admin/aliases/get, which a user
            # granted only what SQL queries need does not have: list none
            # rather than fail reflection (and has_table) for them.
            logger.warning("Not allowed to list aliases (%s); none listed", ex.error)
            aliases_response = []
        # Cast response to list of dicts for type checking
        aliases: List[Dict[str, Any]] = cast(
            List[Dict[str, Any]], list(aliases_response)
        )
        results: List[Tuple[str, ...]] = []
        for item in aliases:
            if item["alias"] not in as_tables:
                results.append((item["alias"], item["index"]))
        self.description = get_description_from_columns(
            [
                {"name": "VIEW_NAME", "type": "text"},
                {"name": "TABLE_NAME", "type": "text"},
            ]
        )
        self._results = deque(results)
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
        self._results = deque(
            self._traverse_mapping(
                response[index_real_name]["mappings"]["properties"], []
            )
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
        Answers SELECT 1 (SQLAlchemy's ping) with a real SQL request.

        The SQL plugin answers SELECT 1 on OpenSearch; the old emulation
        through ``ping()`` (``HEAD /``) needs a cluster privilege, so a user
        allowed to run SQL failed the connection test. OpenDistro releases
        that reject SELECT 1 still fall back to ``ping()``, but a cluster
        without the SQL endpoint fails the test: every query would fail too.

        :return: A cursor with "1" (result from SELECT 1)
        :raises: DatabaseError in case of a connection error
        """
        try:
            self.elastic_query("SELECT 1", paged=False)
        except exceptions.OperationalError:
            raise
        except exceptions.DatabaseError as ex:
            if self._is_missing_sql_endpoint(ex):
                raise exceptions.OperationalError(
                    f"No SQL endpoint at /{self.sql_path}/: {ex}"
                ) from ex
            try:
                res = self.es.ping()
            except ConnectionError:
                raise exceptions.DatabaseError("Connection failed")
            if not res:
                raise exceptions.DatabaseError("Connection failed")
        self._results = deque([(1,)])
        self.description = get_description_from_columns([{"name": "1", "type": "long"}])
        return self

    @check_closed
    def execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "BaseCursor":
        # custom commands call the cluster APIs directly (cat, mapping, info)
        with translate_transport_errors():
            return self._execute(operation, parameters)

    def _execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "BaseCursor":
        cursor = self.custom_sql_to_method_dispatcher(operation)
        if cursor:
            return cursor

        re_table_name = re.match("SHOW VALID_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_valid_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        self._row_cap: Optional[int] = None
        if has_subquery(query):
            query = self._prepare_subqueries(query)
        try:
            results = self.elastic_query(query, paged=self._pages(query))
        except exceptions.DatabaseError as ex:
            if _is_result_window_error(ex) and is_pageable(query):
                results = self._select_past_result_window(query, ex)
            elif self._last_paged and (
                self._is_legacy_paging_failure(ex)
                or self._is_paging_permission_failure(ex)
            ):
                results = self._unpaged_query_or_raise(query, ex)
            else:
                raise
        else:
            results = self._complete_result(query, results)

        columns = results.get("schema")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an elastic sql ep"
            )
        self.description = get_description_from_columns(columns)
        # The SQL plugin pages results by `fetch_size` like Elasticsearch
        # does; later pages must be followed or rows are silently dropped.
        rows = self.fetch_remaining_pages(results, "datarows")
        if self._row_cap is not None:
            rows = rows[: self._row_cap]
        self._results = deque(convert_rows(columns, rows))
        return self

    def _pages(self, query: str) -> bool:
        """
        Whether ``query`` is sent with ``fetch_size``.

        The legacy engine (v2 off) needs fetch_size to take a statement over
        from the v2 engine, and caps its aggregations at 200 buckets anyway.
        In v2 mode only plain SELECTs without LIMIT are paged, and only on
        OpenSearch's ``_plugins/_sql`` on version 2 or later. Older v2 engines
        cannot page: fetch_size hands the statement to the legacy engine,
        whose semantics differ
        (e.g. ORDER BY resolves a column name to a select-list alias).
        """
        if has_subquery(query):
            # The legacy engine ignores the outer WHERE of a statement over a
            # subquery (and caps it at the size limit); only v2 answers it.
            return False
        if not self.v2:
            return True
        return (
            is_pageable(query)
            and outer_clauses(query).limit is None
            and self.sql_path != LEGACY_SQL_PATH
            and self._can_page_v2()
        )

    def _can_page_v2(self) -> bool:
        # OpenSearch 1.x accepts fetch_size but runs on the legacy engine.
        # If discovery is forbidden, keep v2 semantics rather than guessing.
        version = self._cached_server_version()
        return version is not None and version >= (2, 0)

    def elastic_query(self, query: str, paged: bool = True) -> Dict[str, Any]:
        renames: Dict[str, str] = {}
        if paged:
            # The legacy engine resolves ``t.k`` to a select-list alias ``k``;
            # renaming such aliases keeps its ORDER BY on the column.
            query, renames = rename_colliding_aliases(query)
        results = self._query_endpoint(query, paged)
        for column in results.get("schema") or []:
            for key in ("alias", "name"):
                if column.get(key) in renames:
                    column[key] = renames[column[key]]
        return results

    def _prepare_subqueries(self, query: str) -> str:
        """
        Makes a statement over subqueries (e.g. a Superset virtual dataset)
        return its full, correct result on the SQL plugin's v2 engine:

        - a subquery without a LIMIT stops at the size limit (200 rows by
          default on Open Distro) and silently drops rows from the outer
          result, so it gets an explicit LIMIT of one search window, and a
          subquery with more rows than that raises ``DataError``;
        - Open Distro drops the first matching row when an outer WHERE and an
          outer LIMIT meet without an ORDER BY, so there the outer LIMIT is
          applied to the rows instead.
        """
        query, limited = limit_subqueries(query, MAX_RESULT_WINDOW)
        for inner in limited:
            if self._subquery_rows(inner) > MAX_RESULT_WINDOW:
                raise exceptions.DataError(
                    f"A subquery of this statement returns too many rows: the "
                    f"SQL plugin cannot page a subquery past {MAX_RESULT_WINDOW} "
                    f"rows, so the outer result would silently miss rows. "
                    f"Narrow the subquery (e.g. the virtual dataset's SQL) to "
                    f"fewer rows."
                )
        if self.sql_path == LEGACY_SQL_PATH:
            outer = outer_clauses(query)
            if (
                outer.where
                and outer.limit is not None
                and outer.limit_start is not None
                and not outer.order_by
                and not outer.offset
            ):
                self._row_cap = outer.limit
                query = query[: outer.limit_start].rstrip()
        return query

    def _subquery_rows(self, inner: str) -> int:
        """
        Counts the rows of the subquery ``inner`` up to one past the search
        window, which tells a complete window from a cut one. Open Distro
        refuses a LIMIT past the window whatever the row count; there a full
        window counts as one row more, since it may have been cut.
        """
        rows = self._count_rows(inner, MAX_RESULT_WINDOW)
        if rows < MAX_RESULT_WINDOW:
            return rows
        if self.sql_path != LEGACY_SQL_PATH:
            try:
                return self._count_rows(inner, MAX_RESULT_WINDOW + 1)
            except exceptions.DatabaseError as ex:
                if not _is_result_window_error(ex):
                    raise
        return MAX_RESULT_WINDOW + 1

    def _count_rows(self, inner: str, limit: int) -> int:
        counted = self.elastic_query(
            f"SELECT COUNT(*) FROM ({inner}\nLIMIT {limit}\n) AS es_subquery_rows",
            paged=False,
        )
        return (counted.get("datarows") or [[0]])[0][0]

    def _query_endpoint(self, query: str, paged: bool) -> Dict[str, Any]:
        self._last_paged = paged
        try:
            return super().elastic_query(query, paged=paged)
        except exceptions.ProgrammingError as ex:
            if self._sql_path_explicit or self.sql_path == LEGACY_SQL_PATH:
                raise
            if not self._is_missing_sql_endpoint(ex):
                raise
            # Open Distro for Elasticsearch: only the legacy endpoint exists.
            # Remember it for every later cursor of this connection.
            self.sql_path = LEGACY_SQL_PATH
            self._connection_kwargs["_detected_sql_path"] = LEGACY_SQL_PATH
            self._last_paged = paged = paged and self._pages(query)
            return super().elastic_query(query, paged=paged)

    def _is_missing_sql_endpoint(self, error: exceptions.DatabaseError) -> bool:
        """
        Whether ``error`` says the cluster has no SQL endpoint at ``sql_path``.

        OpenSearch answers ``no handler found for uri``. Elasticsearch 7.10
        (Open Distro) routes ``POST /_plugins/_sql/`` to the index API and
        answers ``invalid_index_name_exception`` for the index ``_plugins``,
        or ``index_not_found_exception`` for it when
        ``action.auto_create_index`` is false.
        """
        if "no handler found for uri" in str(error):
            return True
        details = _error_details(error)
        endpoint_root = self.sql_path.strip("/").split("/")[0]
        return (
            details.get("type") in _MISSING_ENDPOINT_ERRORS
            and details.get("index") == endpoint_root
        )

    @staticmethod
    def _is_legacy_paging_failure(error: exceptions.DatabaseError) -> bool:
        """
        Whether ``error`` is the legacy engine failing on a statement it was
        asked to page but cannot (e.g. GROUP BY on a ``text`` field): HTTP 500,
        ``IllegalStateException: invalid value operation on MISSING_VALUE``.
        Any other error (syntax, missing index, 429, other 5xx) is final.
        """
        cause = error.__cause__
        if not isinstance(cause, TransportError) or cause.status_code != 500:
            return False
        details = _error_details(error)
        return details.get("type") == "IllegalStateException" and (
            "MISSING_VALUE" in str(details.get("details"))
        )

    def _complete_result(self, query: str, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns every row of ``query``, or raises ``DataError`` if the server
        cannot return them all. ``results`` is its first answer.

        A cursor in the answer is followed by the caller. Without one:

        - On Open Distro (``_opendistro/_sql``) a plain SELECT without its own
          LIMIT stops at ``opendistro.query.size_limit`` rows, because SQL
          cursors are off by default there. An explicit LIMIT is not capped,
          so the statement is sent again with ``LIMIT`` set to the largest
          window one search returns; above that the cursor is the only way.
        - The legacy engine (``fetch_size`` sent, v2 off) caps aggregations
          at 200 buckets. Such a result is asked for again unpaged, which the
          v2 engine answers up to its own bucket ceiling.
        """
        rows = len(results.get("datarows") or [])
        outer = outer_clauses(query)
        if is_pageable(query) and outer.limit is not None:
            if outer.limit > MAX_RESULT_WINDOW and rows >= MAX_RESULT_WINDOW:
                # A LIMIT above the search window can return just one window,
                # even with a cursor whose next page is empty. Page the same
                # statement without LIMIT, then enforce the original row cap.
                if results.get("cursor"):
                    self.close_elastic_cursor(results["cursor"])
                return self._select_past_result_window(
                    query, exceptions.DataError("Cannot page a LIMIT with OFFSET")
                )
        if results.get("cursor"):
            return results
        if (
            (
                self.sql_path == LEGACY_SQL_PATH
                or (self.v2 and not self._last_paged and not self._can_page_v2())
            )
            and is_pageable(query)
            and not _TRAILING_LIMIT_RE.search(_QUOTED_RE.sub(" ", query))
            and self._may_be_capped(results, rows)
        ):
            return self._fetch_whole_select(query, rows)
        if self.sql_path == LEGACY_SQL_PATH and has_subquery(query):
            # Open Distro does not cap the outer result of a statement over
            # (limited) subqueries.
            return results
        if self._last_paged:
            if not is_pageable(query) and rows == LEGACY_BUCKET_LIMIT:
                return self._aggregate_without_bucket_cap(query)
            return results
        self._check_unpaged_result(query, results)
        return results

    def _may_be_capped(self, results: Dict[str, Any], rows: int) -> bool:
        """Whether an older unpaged SELECT answer may be missing rows."""
        if self._last_paged:
            # The legacy engine reports every hit in ``total``.
            total = results.get("total")
            return isinstance(total, int) and total > rows
        # The v2 engine reports only the rows it returns.
        size_limit = self._cached_size_limit() or OPEN_DISTRO_DEFAULT_SIZE_LIMIT
        return rows >= size_limit

    def _select_past_result_window(
        self, query: str, error: exceptions.DatabaseError
    ) -> Dict[str, Any]:
        """
        Answers a plain SELECT whose own LIMIT is larger than one search
        window, which the cluster refuses ("Result window is too large"),
        e.g. Superset's SQL Lab adding ``LIMIT 100001``. The rows are fetched
        within the window when they fit in it, else with the SQL cursor, and
        cut at the requested LIMIT.
        """
        # Quoted parts and comments are blanked to the same length, so the
        # offset of the LIMIT clause is the same in the original statement.
        bare = _QUOTED_RE.sub(lambda m: " " * len(m.group(0)), query)
        limit = _TRAILING_LIMIT_RE.search(bare)
        if not limit or "OFFSET" in limit.group(0).upper():
            raise error
        self._row_cap = int(limit.group(1))
        return self._fetch_whole_select(query[: limit.start()], None)

    def _fetch_whole_select(
        self, query: str, first_rows: Optional[int]
    ) -> Dict[str, Any]:
        """
        Fetches all rows of a plain SELECT that stopped at the
        size limit: again with an explicit LIMIT (the same engine, so the same
        semantics), and past one search window with an SQL cursor.
        """
        statement = query.rstrip().rstrip(";").rstrip()
        # A newline ends a trailing "--" comment before the LIMIT.
        windowed = f"{statement}\nLIMIT {MAX_RESULT_WINDOW}"
        try:
            results = self.elastic_query(windowed, paged=self._last_paged)
        except exceptions.DatabaseError as ex:
            if "Result window is too large" not in str(ex):
                raise
            results = None
        if (
            results is not None
            and len(results.get("datarows") or []) < MAX_RESULT_WINDOW
        ):
            return results
        # A full window with a cursor is not proof of completeness: on a
        # limited v2 request that cursor can have no remaining rows.
        if results is not None and results.get("cursor"):
            self.close_elastic_cursor(results["cursor"])
        # More rows than one search returns: only an unlimited query can page them.
        try:
            paged = self.elastic_query(query, paged=True)
        except exceptions.DatabaseError as ex:
            raise exceptions.DataError(
                "The SQL plugin cannot page this SELECT past the search window; "
                "add a LIMIT of at most 10000 or narrow the query."
            ) from ex
        if paged.get("cursor"):
            if self.v2 and results is not None:
                # Paging on older servers uses the legacy engine; keep the
                # v2 schema (notably timestamp vs date) from the window probe.
                paged["schema"] = results.get("schema")
            return paged
        raise exceptions.DataError(
            f"The query reached the search result window ({MAX_RESULT_WINDOW} "
            f"rows), and SQL cursors are unavailable for this statement. "
            f"On Open Distro enable opendistro.sql.cursor.enabled; otherwise "
            f"add a LIMIT of at most {MAX_RESULT_WINDOW} or narrow the query."
        )

    def _aggregate_without_bucket_cap(self, query: str) -> Dict[str, Any]:
        """
        Asks the v2 engine for an aggregation the legacy engine may have cut
        at 200 buckets; if it cannot answer, raises ``DataError``.
        """
        try:
            results = self.elastic_query(query, paged=False)
        except exceptions.DatabaseError as ex:
            raise exceptions.DataError(
                f"The SQL plugin's legacy engine returns at most "
                f"{LEGACY_BUCKET_LIMIT} groups, this query reached that many, "
                f"and the v2 engine could not run it ({ex}). Rows may be "
                f"missing: narrow the query or add a LIMIT."
            ) from ex
        self._check_unpaged_result(query, results)
        return results

    def _is_paging_permission_failure(self, error: exceptions.DatabaseError) -> bool:
        """
        Whether OpenSearch refused a paged v2 request for lack of privileges.

        The v2 engine's pagination needs privileges that the unpaged request
        does not (``indices:data/read/search`` beyond the queried index,
        ``indices:admin/aliases/get``), so a user who may query an index
        unpaged gets a 403 once ``fetch_size`` is sent.
        """
        if not self.v2:
            return False
        details = _error_details(error)
        return details.get("type") == "OpenSearchSecurityException" or (
            "OpenSearchSecurityException" in str(error)
        )

    def _unpaged_query_or_raise(
        self, query: str, error: exceptions.DatabaseError
    ) -> Dict[str, Any]:
        """
        Retries unpaged a query the server failed on when asked to page it (see
        ``_is_legacy_paging_failure`` and ``_is_paging_permission_failure``).

        Without ``fetch_size`` the result may be silently cut at
        ``plugins.query.size_limit`` rows, so the retry is only accepted if
        the result cannot have been cut (see ``_check_unpaged_result``). After
        a legacy-engine failure the retry is only made if that limit can be
        read. A v2 user without the privileges to page is also not allowed to
        read cluster settings, so there the unpaged answer is returned as the
        v2 engine gives it, with a warning if the limit cannot be read.
        """
        if self._cached_size_limit() is None and not self._is_paging_permission_failure(
            error
        ):
            raise error
        try:
            results = self.elastic_query(query, paged=False)
        except exceptions.DatabaseError:
            raise error
        self._check_unpaged_result(query, results)
        return results

    def _check_unpaged_result(self, query: str, results: Dict[str, Any]) -> None:
        """
        Raises ``DataError`` if an unpaged result may have been cut: plain
        SELECTs at ``plugins.query.size_limit`` (or the search window with an
        explicit LIMIT), aggregations at their version-dependent bucket ceiling.
        A trailing LIMIT no larger than the ceiling explains the count.
        If the SELECT size limit cannot be read, warn
        instead; the known aggregation ceiling still applies.
        """
        probe = grouped_order_probe(query)
        if probe is not None:
            # A LIMIT of five does not make top-N safe: v2 can sort only the
            # first 1000 (or query.size_limit) groups and discard the winners.
            # Validate the unsorted, unfiltered group listing before accepting
            # the original answer. Legacy top-N is unaffected.
            grouped = self.elastic_query(probe, paged=False)
            self._check_unpaged_result(probe, grouped)
        rows = len(results.get("datarows") or [])
        if not rows or results.get("cursor"):
            return
        aggregation = bool(_AGGREGATION_RE.search(_QUOTED_RE.sub(" ", query)))
        limit = outer_clauses(query).limit
        size_limit: Optional[int]
        if aggregation:
            size_limit = self._aggregation_bucket_limit(rows)
        elif limit is not None:
            # An explicit LIMIT bypasses query.size_limit on the v2 engine,
            # including releases whose default is 200, but not the search window.
            size_limit = MAX_RESULT_WINDOW
        else:
            size_limit = self._cached_size_limit()
        if size_limit is None:
            logger.warning(
                "Unpaged result of %d rows not checked for truncation: "
                "plugins.query.size_limit is not readable",
                rows,
            )
            return
        if rows != size_limit:
            return
        if limit is not None and limit <= size_limit:
            return
        ceiling = (
            "the aggregation bucket limit"
            if aggregation
            else (
                "the search result window"
                if limit is not None
                else "plugins.query.size_limit"
            )
        )
        raise exceptions.DataError(
            f"The SQL plugin can only answer this query unpaged, and its "
            f"result reached {ceiling} ({size_limit} rows), "
            f"so rows may be missing. Add a LIMIT of at most {size_limit} "
            f"or narrow the query."
        )

    def _cached_server_version(self) -> Optional[Tuple[int, ...]]:
        """
        Best-effort version discovery, shared by cursors of a connection.
        Also caches the distribution, which only OpenSearch reports: Open
        Distro (and OpenSearch 1.x with
        ``compatibility.override_main_response_version``) report 7.10.2.
        """
        if "_server_version" not in self._connection_kwargs:
            version, distribution = None, None
            try:
                info = self.es.info()["version"]
                version = tuple(int(part) for part in info["number"].split(".")[:3])
                distribution = info.get("distribution")
            except Exception as ex:  # noqa: B902
                logger.warning("Could not read the SQL server version: %s", ex)
            self._connection_kwargs["_server_distribution"] = distribution
            self._connection_kwargs["_server_version"] = version
        return self._connection_kwargs["_server_version"]

    def _aggregation_bucket_limit(self, rows: int) -> int:
        """
        OpenSearch 2.x/3.x also cap v2 buckets at query.size_limit (200 by
        default on 2.11/2.15, 10000 on newer releases), on either SQL endpoint.
        Open Distro and OpenSearch 1.x have only the separate 1000 ceiling.
        Only discover the server when a smaller size limit could explain
        this result. If discovery is forbidden, refuse the ambiguous answer.
        """
        size_limit = self._cached_size_limit()
        if size_limit is None:
            size_limit = OPEN_DISTRO_DEFAULT_SIZE_LIMIT
        if size_limit < UNPAGED_BUCKET_LIMIT and rows == size_limit:
            version = self._cached_server_version()
            if version is None or (
                self._connection_kwargs.get("_server_distribution") == "opensearch"
                and version >= (2, 0)
            ):
                return size_limit
        return UNPAGED_BUCKET_LIMIT

    def _cached_size_limit(self) -> Optional[int]:
        """``get_size_limit``, read once per connection."""
        if "_size_limit" not in self._connection_kwargs:
            self._connection_kwargs["_size_limit"] = self.get_size_limit()
        return self._connection_kwargs["_size_limit"]

    def get_size_limit(self) -> Optional[int]:
        """
        The cluster's ``plugins.query.size_limit`` (``opendistro.query.size_limit``
        on older clusters), or ``None`` if the user may not read it.
        """
        try:
            settings = self.es.transport.perform_request(
                "GET",
                "/_cluster/settings",
                params={"include_defaults": "true", "flat_settings": "true"},
            )
        except Exception as ex:  # noqa: B902
            logger.warning("Could not read plugins.query.size_limit: %s", ex)
            return None
        if not isinstance(settings, dict):
            return None
        for scope in ("transient", "persistent", "defaults"):
            for name in ("plugins.query.size_limit", "opendistro.query.size_limit"):
                value = (settings.get(scope) or {}).get(name)
                if value is not None:
                    return int(value)
        return None

    def sanitize_query(self, query: str) -> str:
        """
        Removes dummy schema from queries
        """
        query = query.replace(f'FROM "{DEFAULT_SCHEMA}".', "FROM ")
        return query.replace(f"FROM `{DEFAULT_SCHEMA}`.", "FROM ")
