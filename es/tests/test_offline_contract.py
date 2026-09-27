"""
Offline regression tests for SQLAlchemy 2 / DB-API contract fixes.

They mock the transport layer or only compile statements, so they run
without a cluster, in every CI job.
"""

import datetime
import json
import logging
import re
import unittest
from unittest.mock import MagicMock, patch
import warnings

from elasticsearch import exceptions as es_exceptions
from es import baseapi, basesqlalchemy, exceptions
from es.elastic import api as elastic_api
from es.elastic.sqlalchemy import ESDialect, ESHTTPSDialect
from es.opendistro import api as opendistro_api
from es.opendistro.sqlalchemy import (
    ESDialect as ODDialect,
    ESHTTPSDialect as ODHTTPSDialect,
)
from opensearchpy import exceptions as os_exceptions
import sqlalchemy as sa
from sqlalchemy import types
from urllib3 import HTTPResponse

DIALECTS = (ESDialect, ESHTTPSDialect, ODDialect, ODHTTPSDialect)

# Responses captured from live clusters
ODFE_PLUGINS_SQL = (
    400,
    {
        "error": {
            "root_cause": [
                {
                    "type": "invalid_index_name_exception",
                    "reason": "Invalid index name [_plugins], must not start "
                    "with '_', '-', or '+'",
                    "index_uuid": "_na_",
                    "index": "_plugins",
                }
            ],
            "type": "invalid_index_name_exception",
            "reason": "Invalid index name [_plugins], must not start with "
            "'_', '-', or '+'",
            "index_uuid": "_na_",
            "index": "_plugins",
        },
        "status": 400,
    },
)
LEGACY_PAGING_FAILURE = (
    500,
    {
        "error": {
            "reason": "There was internal problem at backend",
            "details": "invalid value operation on MISSING_VALUE",
            "type": "IllegalStateException",
        },
        "status": 500,
    },
)
SYNTAX_ERROR = (
    400,
    {
        "error": {
            "reason": "Invalid SQL query",
            "details": "Query must start with SELECT, DELETE, SHOW or DESCRIBE",
            "type": "SQLFeatureNotSupportedException",
        },
        "status": 400,
    },
)


def sql_answer(rows, cursor=None, column_type="long"):
    body = {
        "schema": [{"name": "k", "type": column_type}],
        "datarows": [[r] for r in rows],
        "total": len(rows),
        "size": len(rows),
        "status": 200,
    }
    if cursor:
        body["cursor"] = cursor
    return 200, body


class FakeCluster:
    """
    Answers the HTTP requests of an OpenSearch client, so that responses go
    through the client's real status and error parsing.
    """

    def __init__(self, cursor, handler):
        self.handler = handler
        self.requests = []
        pool = cursor.es.transport.connection_pool
        self.patch = patch.object(pool.connection.pool, "urlopen", self.urlopen)

    def urlopen(self, method, url, body=None, **kwargs):
        payload = json.loads(body) if body else None
        self.requests.append((method, url.split("?")[0], payload))
        status, answer = self.handler(method, url.split("?")[0], payload)
        return HTTPResponse(
            body=json.dumps(answer).encode(),
            status=status,
            headers={"content-type": "application/json; charset=UTF-8"},
            preload_content=True,
        )

    def __enter__(self):
        self.patch.start()
        return self

    def __exit__(self, *exc):
        self.patch.stop()

    def sql_requests(self):
        return [r for r in self.requests if "_sql" in r[1]]


def flights(schema=None):
    return sa.table(
        "flights", sa.column("Carrier"), sa.column("FlightNum"), schema=schema
    )


class TestDummySchemaIsNotRendered(unittest.TestCase):
    def test_projection_and_from_clause_omit_default_schema(self):
        for dialect_cls in DIALECTS:
            t = flights("default")
            stmt = sa.select(t.c.Carrier).order_by(t.c.FlightNum)
            sql = str(stmt.compile(dialect=dialect_cls()))
            self.assertNotIn("default", sql, dialect_cls)
            self.assertIn("flights", sql)

    def test_other_schemas_are_still_rendered(self):
        for dialect_cls in DIALECTS:
            sql = str(
                sa.select(flights("other").c.Carrier).compile(dialect=dialect_cls())
            )
            self.assertIn("other", sql, dialect_cls)

    def test_schema_translate_map_to_default_is_omitted(self):
        for dialect_cls in DIALECTS:
            dialect = dialect_cls()
            stmt = sa.select(flights().c.Carrier)
            compiled = stmt.compile(
                dialect=dialect, schema_translate_map={None: "default"}
            )
            rendered = compiled.preparer._render_schema_translates(
                compiled.string, {None: "default"}
            )
            self.assertNotIn("default", rendered, dialect_cls)
            self.assertNotIn("__[SCHEMA", rendered, dialect_cls)


class TestDialectFlags(unittest.TestCase):
    def test_statement_cache_is_enabled_on_every_concrete_dialect(self):
        for dialect_cls in DIALECTS:
            self.assertIs(dialect_cls.__dict__.get("supports_statement_cache"), True)
            with warnings.catch_warnings():
                warnings.simplefilter("error", sa.exc.SAWarning)
                sa.select(flights().c.Carrier).compile(dialect=dialect_cls())

    def test_server_version_info_is_read_from_the_cluster(self):
        connection = MagicMock()
        es = connection.connection.dbapi_connection.es
        es.info.return_value = {"version": {"number": "7.17.29"}}
        self.assertEqual(ESDialect()._get_server_version_info(connection), (7, 17, 29))
        es.info.return_value = {"version": {"number": "8.19.4-SNAPSHOT"}}
        self.assertEqual(ESDialect()._get_server_version_info(connection), (8, 19, 4))

    def test_server_version_info_is_best_effort(self):
        connection = MagicMock()
        connection.connection.dbapi_connection.es.info.side_effect = RuntimeError()
        self.assertIsNone(ODDialect()._get_server_version_info(connection))

    def test_has_table_includes_views(self):
        dialect = ESDialect()
        with patch.object(
            dialect, "get_table_names", return_value=["flights"]
        ), patch.object(dialect, "get_view_names", return_value=["flights_alias"]):
            self.assertTrue(dialect.has_table(None, "flights"))
            self.assertTrue(dialect.has_table(None, "flights_alias"))
            self.assertFalse(dialect.has_table(None, "missing"))


class TestReflectedTypes(unittest.TestCase):
    def test_numeric_mapping_types(self):
        expected = {
            "double": types.Float,
            "float": types.Float,
            "half_float": types.Float,
            "scaled_float": types.Float,
            "byte": types.SmallInteger,
            "short": types.SmallInteger,
            "integer": types.Integer,
            "long": types.BigInteger,
            "unsigned_long": types.BigInteger,
            "date_nanos": types.DateTime,
            "binary": types.String,
        }
        for es_type, sa_type in expected.items():
            self.assertIsInstance(basesqlalchemy.get_type(es_type), sa_type, es_type)

    def test_double_is_not_rounded_by_the_result_processor(self):
        type_ = basesqlalchemy.get_type("double")
        processor = type_.result_processor(ESDialect(), None)
        value = 1.2345678901234e-05
        self.assertEqual(processor(value) if processor else value, value)


class TestResultTypes(unittest.TestCase):
    def test_unknown_result_type_does_not_raise(self):
        for name in ("null", "undefined", "byte", "unsigned_long", "something_new"):
            self.assertIsInstance(baseapi.get_type(name), int, name)

    def test_elasticsearch_datetime_keeps_its_offset(self):
        columns = [{"name": "ts", "type": "datetime"}, {"name": "s", "type": "keyword"}]
        row = baseapi.convert_rows(
            columns, [("2026-01-02T11:30:00.123Z", "2026-01-02T11:30:00.123Z")]
        )[0]
        self.assertEqual(
            row[0],
            datetime.datetime(2026, 1, 2, 11, 30, 0, 123000, datetime.timezone.utc),
        )
        # only temporal columns are converted
        self.assertEqual(row[1], "2026-01-02T11:30:00.123Z")
        shifted = baseapi.convert_rows(
            columns, [("2026-01-02T13:30:00.123+02:00", None)]
        )[0][0]
        self.assertEqual(shifted.utcoffset(), datetime.timedelta(hours=2))

    def test_opensearch_timestamp_date_and_time(self):
        columns = [
            {"name": "ts", "type": "timestamp"},
            {"name": "d", "type": "date"},
            {"name": "t", "type": "time"},
        ]
        self.assertEqual(
            baseapi.convert_rows(
                columns, [("2026-01-02 11:30:00.123", "2026-01-02", "11:30:00.123")]
            ),
            [
                (
                    datetime.datetime(2026, 1, 2, 11, 30, 0, 123000),
                    datetime.date(2026, 1, 2),
                    datetime.time(11, 30, 0, 123000),
                )
            ],
        )

    def test_elasticsearch_date_is_a_date(self):
        self.assertEqual(
            baseapi.convert_rows(
                [{"name": "d", "type": "date"}], [("2026-01-02T00:00:00.000Z",)]
            ),
            [(datetime.date(2026, 1, 2),)],
        )

    def test_a_column_is_never_mixed(self):
        columns = [
            {"name": "ts", "type": "datetime"},
            {"name": "o", "type": "datetime"},
        ]
        nanos = "2026-01-02T11:30:00.123456789Z"
        whole = "2026-01-02T11:30:00.123456000Z"
        rows = baseapi.convert_rows(columns, [(nanos, whole), (whole, None)])
        # a value datetime cannot hold keeps the whole column as strings
        self.assertEqual([r[0] for r in rows], [nanos, whole])
        # trailing zero nanoseconds lose nothing
        self.assertEqual(rows[0][1].microsecond, 123456)
        self.assertIsNone(rows[1][1])
        self.assertEqual(
            baseapi.convert_rows(columns[:1], [("not a date",)]), [("not a date",)]
        )

    def test_rows_untouched_without_temporal_columns(self):
        rows = [(1,)]
        self.assertIs(baseapi.convert_rows([{"name": "a", "type": "long"}], rows), rows)


class TestTransportErrors(unittest.TestCase):
    def setUp(self):
        self.cursor = elastic_api.connect(host="localhost").cursor()

    def execute_raising(self, error):
        with patch.object(
            self.cursor.es.transport, "perform_request", side_effect=error
        ):
            self.cursor.execute("select 1")

    def test_authentication_failure_is_a_dbapi_error(self):
        error = es_exceptions.AuthenticationException(
            401, "security_exception", {"error": "unable to authenticate"}
        )
        with self.assertRaises(exceptions.OperationalError) as ctx:
            self.execute_raising(error)
        self.assertIs(ctx.exception.__cause__, error)

    def test_tls_failure_keeps_its_cause_in_the_message(self):
        error = es_exceptions.SSLError(
            "N/A", "certificate verify failed", Exception("self-signed")
        )
        with self.assertRaises(exceptions.OperationalError) as ctx:
            self.execute_raising(error)
        self.assertIn("certificate verify failed", str(ctx.exception))

    def test_other_server_errors_are_database_errors(self):
        error = es_exceptions.TransportError(500, "internal", {})
        with self.assertRaises(exceptions.DatabaseError):
            self.execute_raising(error)

    def test_listing_tolerates_missing_index_stats_privilege(self):
        show_tables = {
            "columns": [
                {"name": "name", "type": "keyword"},
                {"name": "type", "type": "keyword"},
            ],
            "rows": [["flights", "TABLE"], ["old", "BASE TABLE"], ["v", "VIEW"]],
        }
        denied = es_exceptions.AuthorizationException(403, "security_exception", {})
        with patch.object(
            self.cursor.es.transport, "perform_request", return_value=show_tables
        ), patch.object(self.cursor.es.cat, "indices", side_effect=denied):
            tables = self.cursor.execute("SHOW VALID_TABLES").fetchall()
        self.assertEqual([t[0] for t in tables], ["flights", "old"])


class TestOpenSearchCursor(unittest.TestCase):
    def cursor(self, **kwargs):
        return opendistro_api.connect(host="localhost", **kwargs).cursor()

    def test_v2_url_values(self):
        for value in ("true", "True", "1", "yes", "ON"):
            self.assertTrue(self.cursor(v2=value).v2, value)
        for value in ("false", "False", "0", "no", "off", ""):
            self.assertFalse(self.cursor(v2=value).v2, value)
        self.assertTrue(self.cursor(v2=True).v2)
        self.assertIsNotNone(self.cursor(v2="true").fetch_size)

    def test_select_one_is_real_sql(self):
        cursor = self.cursor()
        answer = {"schema": [{"name": "1", "type": "integer"}], "datarows": [[1]]}
        with patch.object(
            cursor.es.transport, "perform_request", return_value=answer
        ) as request, patch.object(cursor.es, "ping") as ping:
            self.assertEqual(cursor.execute("SELECT 1").fetchall(), [(1,)])
        ping.assert_not_called()
        self.assertEqual(request.call_args.kwargs["body"]["query"], "SELECT 1")

    def test_select_one_falls_back_to_ping_when_sql_rejects_it(self):
        cursor = self.cursor()
        rejected = os_exceptions.RequestError(400, "unsupported", {})
        with patch.object(
            cursor.es.transport, "perform_request", side_effect=rejected
        ), patch.object(cursor.es, "ping", return_value=True):
            self.assertEqual(cursor.execute("SELECT 1").fetchall(), [(1,)])

    def _time_zone_payload(self, time_zone):
        cursor = self.cursor(time_zone=time_zone)
        with FakeCluster(cursor, lambda *_: sql_answer([1])) as cluster:
            cursor.execute("select k from t")
        return cluster.sql_requests()[0][2]

    def test_utc_time_zone_is_accepted_silently(self):
        for time_zone in ("UTC", "utc", "Z", "+00:00", "-00:00", "Etc/UTC", "GMT"):
            with self.assertNoLogs("es.opendistro.api", logging.WARNING):
                payload = self._time_zone_payload(time_zone)
            self.assertNotIn("time_zone", payload, time_zone)

    def test_other_time_zone_warns_and_is_dropped(self):
        for time_zone in ("+02:00", "Europe/Lisbon"):
            with self.assertLogs("es.opendistro.api", logging.WARNING) as logs:
                payload = self._time_zone_payload(time_zone)
            self.assertIn("time_zone", logs.output[0])
            self.assertNotIn("time_zone", payload)

    def test_pages_are_followed(self):
        cursor = self.cursor()
        responses = [
            {
                "schema": [{"name": "n", "type": "long"}],
                "datarows": [[1]],
                "cursor": "c",
            },
            {"datarows": [[2]]},
        ]
        with patch.object(
            cursor.es.transport, "perform_request", side_effect=responses
        ):
            self.assertEqual(cursor.execute("select n from t").fetchall(), [(1,), (2,)])

    def _run(self, query, size_limit=10, paged_answer=None, unpaged_rows=5):
        """
        Runs ``query`` against a fake cluster whose paged requests get
        ``paged_answer`` (a status/body pair), unpaged ones ``unpaged_rows``
        rows, and whose plugins.query.size_limit is ``size_limit`` (None:
        not readable). Runs in v2 mode.
        """
        cursor = self.cursor(v2="true")

        def handler(method, path, payload):
            if path == "/_cluster/settings":
                if size_limit is None:
                    return 403, {"error": {"type": "security_exception"}}
                return 200, {"defaults": {"plugins.query.size_limit": size_limit}}
            if "fetch_size" in payload:
                return paged_answer or sql_answer(range(unpaged_rows))
            return sql_answer(range(unpaged_rows))

        with FakeCluster(cursor, handler) as cluster:
            try:
                return cursor.execute(query).fetchall()
            finally:
                self.requests = cluster.requests

    def test_legacy_paging_failure_is_retried_unpaged(self):
        rows = self._run("select k from t", paged_answer=LEGACY_PAGING_FAILURE)
        self.assertEqual(len(rows), 5)
        self.assertEqual(
            [r[1] for r in self.requests],
            ["/_plugins/_sql/", "/_cluster/settings", "/_plugins/_sql/"],
        )

    def test_unpaged_retry_refused_when_it_may_be_truncated(self):
        with self.assertRaises(exceptions.DataError):
            self._run(
                "select k from t", paged_answer=LEGACY_PAGING_FAILURE, unpaged_rows=10
            )

    def test_original_error_when_size_limit_is_unreadable(self):
        with self.assertRaises(exceptions.DatabaseError) as ctx:
            self._run(
                "select k from t", paged_answer=LEGACY_PAGING_FAILURE, size_limit=None
            )
        self.assertNotIsInstance(ctx.exception, exceptions.DataError)
        self.assertIn("MISSING_VALUE", str(ctx.exception))

    def test_other_errors_are_not_retried(self):
        failures = {
            "syntax": SYNTAX_ERROR,
            "missing index": (404, {"error": {"type": "IndexNotFoundException"}}),
            "circuit breaker": (
                429,
                {"error": {"type": "circuit_breaking_exception"}, "status": 429},
            ),
            "server error": (
                500,
                {"error": {"type": "IllegalStateException", "details": "boom"}},
            ),
        }
        for name, failure in failures.items():
            with self.assertRaises(exceptions.DatabaseError, msg=name):
                self._run("select k from t", paged_answer=failure)
            self.assertEqual(
                [r[1] for r in self.requests if r[1] != "/"],
                ["/_plugins/_sql/"],
                name,
            )

    def test_group_by_is_sent_unpaged_and_returns_every_bucket(self):
        for v2 in ("true", "1", True):
            cursor = self.cursor(v2=v2)
            keys = [f"key{i:03d}" for i in range(450)]

            def handler(method, path, payload):
                if path == "/_cluster/settings":
                    return 200, {"defaults": {"plugins.query.size_limit": "200"}}
                if "fetch_size" in payload:
                    # the legacy engine: top 200 buckets, no cursor
                    return sql_answer(keys[:200], column_type="keyword")
                return sql_answer(keys, column_type="keyword")

            with FakeCluster(cursor, handler) as cluster:
                rows = cursor.execute(
                    "SELECT k, COUNT(*) AS c FROM grp GROUP BY k"
                ).fetchall()
            self.assertEqual(len(rows), 450, v2)
            self.assertNotIn("fetch_size", cluster.sql_requests()[0][2], v2)

    def test_legacy_engine_mode_keeps_fetch_size(self):
        # without v2, fetch_size is what selects the legacy engine
        cursor = self.cursor(v2="false")
        with FakeCluster(cursor, lambda *_: sql_answer([1])) as cluster:
            cursor.execute("SELECT k, COUNT(*) FROM grp GROUP BY k")
        self.assertIn("fetch_size", cluster.sql_requests()[0][2])

    def test_unpaged_result_at_the_size_limit_is_refused(self):
        with self.assertRaises(exceptions.DataError):
            self._run("select distinct k from t", unpaged_rows=10)
        # an explicit LIMIT explains the count
        rows = self._run("select distinct k from t LIMIT 10", unpaged_rows=10)
        self.assertEqual(len(rows), 10)
        # an unreadable size limit is not an error for an unpaged statement
        rows = self._run("select distinct k from t", unpaged_rows=10, size_limit=None)
        self.assertEqual(len(rows), 10)

    def test_paging_refused_for_privileges_is_retried_unpaged(self):
        # OpenSearch's v2 pagination needs privileges that an unpaged query
        # does not; a user who can query the index gets a 403 once paged.
        denied = (
            403,
            {
                "error": {
                    "reason": "no permissions for [indices:data/read/search]",
                    "type": "OpenSearchSecurityException",
                },
                "status": 403,
            },
        )
        rows = self._run("select k from t", paged_answer=denied, size_limit=None)
        self.assertEqual(len(rows), 5)
        sql = [r for r in self.requests if r[1] == "/_plugins/_sql/"]
        self.assertIn("fetch_size", sql[0][2])
        self.assertNotIn("fetch_size", sql[1][2])
        # the unpaged answer is still refused when it may have been cut
        with self.assertRaises(exceptions.DataError):
            self._run("select k from t", paged_answer=denied, unpaged_rows=10)

    def test_v2_on_the_legacy_endpoint_is_not_paged(self):
        # Open Distro's v2 engine cannot page: fetch_size would hand a plain
        # SELECT to the legacy engine, which resolves ORDER BY names to aliases.
        for kwargs in ({"sql_path": "_opendistro/_sql"}, {}):
            cursor = self.cursor(v2="true", **kwargs)

            def handler(method, path, payload):
                if path == "/_cluster/settings":
                    return 200, {"defaults": {"opendistro.query.size_limit": "200"}}
                if path.startswith("/_plugins/"):
                    return ODFE_PLUGINS_SQL
                return sql_answer(range(3))

            with FakeCluster(cursor, handler) as cluster:
                rows = cursor.execute("select k from t order by k").fetchall()
            self.assertEqual(len(rows), 3, kwargs)
            legacy = [r for r in cluster.requests if r[1] == "/_opendistro/_sql/"]
            self.assertTrue(legacy, kwargs)
            self.assertTrue(all("fetch_size" not in r[2] for r in legacy), kwargs)

    def test_plain_select_is_paged(self):
        rows = self._run("select k from t", unpaged_rows=3)
        self.assertEqual(len(rows), 3)
        self.assertIn("fetch_size", self.requests[0][2])
        # a paged result is complete: the size limit is never read
        self.assertNotIn("/_cluster/settings", [r[1] for r in self.requests])

    def test_listing_aliases_tolerates_missing_privilege(self):
        cursor = self.cursor()
        denied = os_exceptions.AuthorizationException(403, "security_exception", {})
        with patch.object(cursor.es.cat, "aliases", side_effect=denied):
            self.assertEqual(cursor.execute("SHOW VALID_VIEWS").fetchall(), [])

    def test_has_table_without_alias_privilege(self):
        engine = sa.create_engine("odelasticsearch+http://localhost:9200/")
        denied = os_exceptions.AuthorizationException(403, "security_exception", {})
        tables = {
            "schema": [
                {"name": "TABLE_CAT", "type": "keyword"},
                {"name": "TABLE_SCHEM", "type": "keyword"},
                {"name": "TABLE_NAME", "type": "keyword"},
            ],
            "datarows": [["c", None, "flights"]],
        }
        with patch(
            "opensearchpy.transport.Transport.perform_request", return_value=tables
        ), patch(
            "opensearchpy.client.cat.CatClient.aliases", side_effect=denied
        ), patch(
            "opensearchpy.client.cat.CatClient.indices", return_value=[]
        ), patch.object(
            ODDialect, "_get_server_version_info", return_value=None
        ):
            with engine.connect() as connection:
                self.assertFalse(engine.dialect.has_table(connection, "missing"))
                self.assertTrue(engine.dialect.has_table(connection, "flights"))


class TestIsPageable(unittest.TestCase):
    def test_plain_selects_are_pageable(self):
        for query in (
            "SELECT a, b FROM t WHERE c > 1 ORDER BY a LIMIT 10",
            "select `count` from t",
            "SELECT a FROM t WHERE b = 'GROUP BY x' -- DISTINCT",
            "SELECT DATE_FORMAT(ts, 'yyyy') AS y FROM t",
        ):
            self.assertTrue(opendistro_api.is_pageable(query), query)

    def test_other_statements_are_not(self):
        for query in (
            "SELECT k, COUNT(*) FROM grp GROUP BY k",
            "select count(*) from t",
            "SELECT SUM(v) FROM t",
            "SELECT k FROM t GROUP  BY k HAVING k > 1",
            "SELECT DISTINCT k FROM t",
            "SELECT a.k FROM t a JOIN u b ON a.k = b.k",
            "SELECT k FROM t UNION SELECT k FROM u",
            "SELECT k FROM (SELECT k FROM t) s",
            "SELECT ROW_NUMBER() OVER (ORDER BY k) FROM t",
            "SHOW TABLES LIKE %",
            "DESCRIBE TABLES LIKE t",
        ):
            self.assertFalse(opendistro_api.is_pageable(query), query)


class TestPagination(unittest.TestCase):
    def test_stops_on_an_empty_page_that_still_has_a_cursor(self):
        cursor = opendistro_api.connect(host="localhost").cursor()

        def handler(method, path, payload):
            if "query" in payload:
                return sql_answer([1], cursor="c1")
            if path.endswith("/close"):
                return 200, {"succeeded": True}
            return 200, {"datarows": [], "cursor": "c2", "status": 200}

        with FakeCluster(cursor, handler) as cluster:
            self.assertEqual(cursor.execute("select k from t").fetchall(), [(1,)])
        self.assertEqual(
            [(r[1], r[2]) for r in cluster.requests[1:]],
            [
                ("/_plugins/_sql/", {"cursor": "c1"}),
                ("/_plugins/_sql/close", {"cursor": "c2"}),
            ],
        )

    def test_a_repeated_cursor_is_followed(self):
        # Elasticsearch returns the same cursor for every page of a result
        cursor = elastic_api.connect(host="localhost").cursor()
        pages = [
            {"columns": [{"name": "a", "type": "long"}], "rows": [[1]], "cursor": "c"},
            {"rows": [[2]], "cursor": "c"},
            {"rows": [[3]]},
        ]
        with patch.object(cursor.es.transport, "perform_request", side_effect=pages):
            self.assertEqual(
                cursor.execute("select a from t").fetchall(), [(1,), (2,), (3,)]
            )


class TestParseBool(unittest.TestCase):
    def test_values(self):
        for value in ("true", "True", "1", "yes", "on", " ON "):
            self.assertIs(baseapi.parse_bool_argument(value), True, value)
        for value in ("false", "False", "0", "no", "off"):
            self.assertIs(baseapi.parse_bool_argument(value), False, value)
        with self.assertRaises(ValueError):
            baseapi.parse_bool_argument("maybe")
        self.assertIs(basesqlalchemy.parse_bool_argument, baseapi.parse_bool_argument)


class TestOpenSearchEndpointAndQualifiers(unittest.TestCase):
    def test_modern_endpoint_by_default_with_legacy_fallback(self):
        conn = opendistro_api.connect(host="localhost")
        cursor = conn.cursor()
        self.assertEqual(cursor.sql_path, "_plugins/_sql")
        missing = os_exceptions.RequestError(
            400, "no handler found for uri [/_plugins/_sql/] and method [POST]", {}
        )
        answer = {"schema": [{"name": "a", "type": "long"}], "datarows": [[1]]}
        with patch.object(
            cursor.es.transport, "perform_request", side_effect=[missing, answer]
        ) as request:
            self.assertEqual(cursor.execute("select a from t").fetchall(), [(1,)])
        self.assertEqual(
            [c.args[1] for c in request.call_args_list],
            ["/_plugins/_sql/", "/_opendistro/_sql/"],
        )
        # remembered for the connection's later cursors
        self.assertEqual(conn.cursor().sql_path, "_opendistro/_sql")

    @staticmethod
    def odfe(method, path, payload):
        # Open Distro 1.13.2 (Elasticsearch 7.10.2)
        if path.startswith("/_plugins/"):
            return ODFE_PLUGINS_SQL
        if path == "/_opendistro/_sql/":
            return sql_answer([1])
        return 404, {}

    def test_open_distro_falls_back_to_the_legacy_endpoint(self):
        conn = opendistro_api.connect(host="localhost")
        cursor = conn.cursor()
        with FakeCluster(cursor, self.odfe) as cluster:
            self.assertEqual(cursor.execute("select k from t").fetchall(), [(1,)])
            self.assertEqual(conn.cursor().execute("SELECT 1").fetchall(), [(1,)])
        self.assertEqual(
            [r[1] for r in cluster.requests],
            ["/_plugins/_sql/", "/_opendistro/_sql/", "/_opendistro/_sql/"],
        )

    def test_select_one_on_open_distro_uses_the_legacy_endpoint(self):
        cursor = opendistro_api.connect(host="localhost").cursor()
        with FakeCluster(cursor, self.odfe) as cluster, patch.object(
            cursor.es, "ping"
        ) as ping:
            self.assertEqual(cursor.execute("SELECT 1").fetchall(), [(1,)])
        ping.assert_not_called()
        self.assertEqual(cluster.requests[-1][1], "/_opendistro/_sql/")

    def test_select_one_fails_without_a_sql_endpoint(self):
        # Elasticsearch without the SQL plugin: neither endpoint exists
        def no_sql(method, path, payload):
            status, body = ODFE_PLUGINS_SQL
            index = path.strip("/").split("/")[0]
            return status, json.loads(json.dumps(body).replace("_plugins", index))

        for sql_path in (None, "_opendistro/_sql"):
            cursor = opendistro_api.connect(
                host="localhost", sql_path=sql_path
            ).cursor()
            with FakeCluster(cursor, no_sql), patch.object(
                cursor.es, "ping", return_value=True
            ) as ping:
                with self.assertRaises(exceptions.OperationalError):
                    cursor.execute("SELECT 1")
            ping.assert_not_called()

    def test_other_index_name_errors_are_not_a_missing_endpoint(self):
        cursor = opendistro_api.connect(host="localhost").cursor()

        def other(method, path, payload):
            status, body = ODFE_PLUGINS_SQL
            return status, json.loads(json.dumps(body).replace('"_plugins"', '"_x"'))

        with FakeCluster(cursor, other) as cluster:
            with self.assertRaises(exceptions.ProgrammingError):
                cursor.execute("select k from t")
        self.assertEqual(len(cluster.requests), 1)

    def test_explicit_sql_path_is_never_replaced(self):
        cursor = opendistro_api.connect(
            host="localhost", sql_path="_plugins/_sql"
        ).cursor()
        missing = os_exceptions.RequestError(400, "no handler found for uri", {})
        with patch.object(cursor.es.transport, "perform_request", side_effect=missing):
            with self.assertRaises(exceptions.ProgrammingError):
                cursor.execute("select a from t")

    def test_single_table_columns_are_unqualified(self):
        t = sa.table("flights", sa.column("a"), sa.column("b"))
        sql = str(sa.select(t.c.a).order_by(t.c.b).compile(dialect=ODDialect()))
        self.assertNotIn("flights.", sql)
        self.assertIn("ORDER BY b", sql)

    def test_qualifier_kept_when_a_label_shadows_the_column(self):
        t = sa.table("grp", sa.column("k"), sa.column("v"))
        stmt = (
            sa.select(t.c.v.label("k"), t.c.k.label("key"))
            .order_by(t.c.k.desc())
            .limit(3)
        )
        sql = " ".join(str(stmt.compile(dialect=ODDialect())).split())
        self.assertIn("ORDER BY grp.k DESC", sql)
        self.assertIn("SELECT v AS k", sql)
        # a label naming its own column is no collision
        stmt = sa.select(t.c.k.label("k")).group_by(t.c.k).order_by(t.c.k)
        sql = " ".join(str(stmt.compile(dialect=ODDialect())).split())
        self.assertEqual(sql, "SELECT k AS k FROM grp GROUP BY k ORDER BY k")

    def test_correlated_references_keep_their_qualifier(self):
        outer = sa.table("t1", sa.column("x"))
        inner = sa.table("t2", sa.column("x"))
        exists = sa.exists().where(inner.c.x == outer.c.x)
        sql = str(sa.select(outer.c.x).where(exists).compile(dialect=ODDialect()))
        self.assertIn("WHERE x = t1.x", sql.split("EXISTS")[1])


if __name__ == "__main__":
    unittest.main()


class FakeOpenDistro:
    """
    Open Distro 1.13 SQL as observed live: without a LIMIT a query stops at
    opendistro.query.size_limit (200); the legacy engine (``fetch_size``
    sent) reports every hit in ``total``, the v2 engine only what it returns;
    an explicit LIMIT is not capped, up to the 10000-row search window; the
    legacy engine caps aggregations at 200 buckets; SQL cursors are off by
    default.
    """

    def __init__(self, rows, cursors=False):
        self.rows = rows
        self.cursors = cursors

    def __call__(self, method, path, payload):
        if path == "/_cluster/settings":
            return 200, {"defaults": {"opendistro.query.size_limit": "200"}}
        if path.startswith("/_plugins/"):
            return ODFE_PLUGINS_SQL
        if "cursor" in payload:
            start = int(payload["cursor"])
            page = list(range(start, min(start + 1000, self.rows)))
            nxt = start + 1000
            return sql_answer(page, cursor=str(nxt) if nxt < self.rows else None)
        query = payload["query"]
        legacy = "fetch_size" in payload
        if "GROUP BY" in query:
            return sql_answer(range(200 if legacy else self.rows))
        limit = re.search(r"LIMIT (\d+)(?: OFFSET \d+)?\s*$", query)
        if limit:
            n = int(limit.group(1))
            if n > 10000:
                return 500, {
                    "error": {
                        "type": "SearchPhaseExecutionException",
                        "reason": "all shards failed",
                        "details": "Result window is too large, from + size must "
                        "be less than or equal to: [10000]",
                    },
                    "status": 500,
                }
            return sql_answer(range(min(n, self.rows)))
        if legacy and self.cursors and self.rows > payload["fetch_size"]:
            return sql_answer(range(1000), cursor="1000")
        status, body = sql_answer(range(min(200, self.rows)))
        if legacy:
            body["total"] = self.rows
        return status, body


class TestOpenDistroReturnsEveryRow(unittest.TestCase):
    def run_query(self, query, rows, cursors=False, **kwargs):
        cursor = opendistro_api.connect(host="localhost", **kwargs).cursor()
        with FakeCluster(cursor, FakeOpenDistro(rows, cursors)) as cluster:
            try:
                return cursor.execute(query).fetchall()
            finally:
                self.requests = cluster.sql_requests()

    def odfe_requests(self):
        # the first request of a connection also probes _plugins/_sql
        return [r for r in self.requests if r[1].startswith("/_opendistro/")]

    def test_plain_select_past_the_size_limit(self):
        for v2 in ("true", "false"):
            rows = self.run_query("SELECT k FROM grp", 450, v2=v2)
            self.assertEqual([r[0] for r in rows], list(range(450)), v2)
            self.assertTrue(self.requests[-1][2]["query"].endswith("LIMIT 10000"))

    def test_v2_retry_stays_on_the_v2_engine(self):
        self.run_query("SELECT k FROM grp ORDER BY k -- comment", 450, v2="true")
        retry = self.requests[-1][2]
        self.assertNotIn("fetch_size", retry)
        self.assertEqual(
            retry["query"], "SELECT k FROM grp ORDER BY k -- comment\nLIMIT 10000"
        )

    def test_complete_answers_are_not_asked_again(self):
        for v2 in ("true", "false"):
            rows = self.run_query("SELECT k FROM grp", 56, v2=v2)
            self.assertEqual(len(rows), 56)
            self.assertEqual(len(self.odfe_requests()), 1)
        rows = self.run_query("SELECT k FROM grp LIMIT 300", 450, v2="true")
        self.assertEqual(len(rows), 300)
        self.assertEqual(len(self.odfe_requests()), 1)

    def test_beyond_one_search_window_uses_the_cursor(self):
        for v2 in ("true", "false"):
            rows = self.run_query("SELECT k FROM big", 12000, cursors=True, v2=v2)
            self.assertEqual(sorted(r[0] for r in rows), list(range(12000)), v2)

    def test_beyond_one_search_window_without_cursors_raises(self):
        for v2 in ("true", "false"):
            with self.assertRaises(exceptions.DataError) as ctx:
                self.run_query("SELECT k FROM big", 12000, v2=v2)
            self.assertIn("opendistro.sql.cursor.enabled", str(ctx.exception))

    def test_legacy_aggregation_at_the_bucket_cap_is_asked_of_the_v2_engine(self):
        rows = self.run_query("SELECT k, COUNT(*) FROM grp GROUP BY k", 450, v2="false")
        self.assertEqual(len(rows), 450)
        self.assertNotIn("fetch_size", self.requests[-1][2])


class TestLimitPastTheSearchWindow(TestOpenDistroReturnsEveryRow):
    # e.g. Superset's SQL Lab appends LIMIT <row limit + 1>, up to 100001

    def test_rows_that_fit_in_one_window(self):
        for v2 in ("true", "false"):
            rows = self.run_query("SELECT k FROM grp LIMIT 100001", 450, v2=v2)
            self.assertEqual(len(rows), 450, v2)
            self.assertTrue(self.requests[-1][2]["query"].endswith("\nLIMIT 10000"))

    def test_rows_past_one_window_use_the_cursor_and_the_limit(self):
        rows = self.run_query(
            "SELECT k FROM big WHERE s = 'LIMIT 3' LIMIT 11000", 12000, cursors=True
        )
        self.assertEqual(len(rows), 11000)

    def test_rows_past_one_window_without_cursors_raise(self):
        with self.assertRaises(exceptions.DataError) as ctx:
            self.run_query("SELECT k FROM big LIMIT 100001", 12000, v2="true")
        self.assertIn("opendistro.sql.cursor.enabled", str(ctx.exception))

    def test_offset_is_not_rewritten(self):
        with self.assertRaises(exceptions.DatabaseError):
            self.run_query("SELECT k FROM grp LIMIT 20000 OFFSET 5", 450, v2="true")


class TestStatementRewrites(unittest.TestCase):
    def test_alias_colliding_with_a_qualified_column_is_renamed(self):
        from es.opendistro.sqltext import rename_colliding_aliases

        sql, renames = rename_colliding_aliases(
            "SELECT v AS k, grp.k AS key FROM grp ORDER BY grp.k DESC, k LIMIT 3"
        )
        self.assertEqual(
            sql,
            "SELECT v AS k__es0, grp.k AS key FROM grp"
            " ORDER BY grp.k DESC, k__es0 LIMIT 3",
        )
        self.assertEqual(renames, {"k__es0": "k"})
        for untouched in (
            "SELECT grp.k AS k FROM grp ORDER BY grp.k",
            "SELECT v AS k FROM grp ORDER BY k",
            "SELECT v AS k FROM grp WHERE s = 'grp.k'",
        ):
            self.assertEqual(rename_colliding_aliases(untouched), (untouched, {}))

    def test_subqueries_without_a_limit_get_one(self):
        from es.opendistro.sqltext import limit_subqueries

        sql, limited = limit_subqueries(
            "SELECT t.v FROM (SELECT k, v FROM grp WHERE s = ')') AS t LIMIT 5", 10000
        )
        self.assertEqual(
            sql,
            "SELECT t.v FROM (SELECT k, v FROM grp WHERE s = ')'\nLIMIT 10000\n)"
            " AS t LIMIT 5",
        )
        self.assertEqual(len(limited), 1)
        own = "SELECT t.v FROM (SELECT v FROM grp LIMIT 30) AS t"
        self.assertEqual(limit_subqueries(own, 10000), (own, []))

    def test_outer_clauses_ignore_subqueries(self):
        from es.opendistro.sqltext import outer_clauses

        outer = outer_clauses(
            "SELECT t.v FROM (SELECT v FROM g WHERE a = 1 ORDER BY v LIMIT 5) AS t"
            " WHERE t.v > 1 LIMIT 100"
        )
        self.assertTrue(outer.where)
        self.assertFalse(outer.order_by)
        self.assertEqual(outer.limit, 100)


class FakeSubqueryCluster:
    """Records statements; answers COUNT(*) probes with ``inner_rows``."""

    def __init__(self, inner_rows, open_distro=True):
        self.inner_rows = inner_rows
        self.open_distro = open_distro

    def __call__(self, method, path, payload):
        if path == "/_cluster/settings":
            return 200, {"defaults": {"opendistro.query.size_limit": "200"}}
        if self.open_distro and path.startswith("/_plugins/"):
            return ODFE_PLUGINS_SQL
        if payload["query"].startswith("SELECT COUNT(*) FROM ("):
            return sql_answer([min(self.inner_rows, 10000)])
        return sql_answer(range(7))


class TestSubqueriesReturnCorrectResults(unittest.TestCase):
    VIRTUAL = (
        "SELECT virtual_table.v FROM (SELECT k, v FROM grp) AS virtual_table "
        "WHERE virtual_table.v > 10 LIMIT 3"
    )

    def run_query(self, query, inner_rows=450, open_distro=True, **kwargs):
        cursor = opendistro_api.connect(host="localhost", **kwargs).cursor()
        fake = FakeSubqueryCluster(inner_rows, open_distro)
        with FakeCluster(cursor, fake) as cluster:
            try:
                return cursor.execute(query).fetchall()
            finally:
                self.sent = [r[2] for r in cluster.sql_requests()]

    def test_open_distro_virtual_dataset(self):
        for v2 in ("true", "false"):
            rows = self.run_query(self.VIRTUAL, v2=v2)
            # the outer LIMIT is applied to the rows, not sent with the WHERE
            self.assertEqual(len(rows), 3, v2)
            final = self.sent[-1]
            self.assertNotIn("fetch_size", final, v2)
            self.assertIn("FROM grp\nLIMIT 10000\n)", final["query"])
            self.assertTrue(final["query"].endswith("virtual_table.v > 10"), v2)
            self.assertTrue(
                any(s["query"].startswith("SELECT COUNT(*) FROM (") for s in self.sent)
            )

    def test_opensearch_keeps_the_outer_limit(self):
        self.run_query(self.VIRTUAL, open_distro=False, v2="false")
        final = self.sent[-1]
        self.assertNotIn("fetch_size", final)
        self.assertTrue(final["query"].endswith("LIMIT 3"))

    def test_subquery_past_one_search_window_raises(self):
        for open_distro in (True, False):
            with self.assertRaises(exceptions.DataError) as ctx:
                self.run_query(self.VIRTUAL, inner_rows=12000, open_distro=open_distro)
            self.assertIn("subquery", str(ctx.exception))

    def test_legacy_engine_gets_a_renamed_colliding_alias(self):
        cursor = opendistro_api.connect(host="localhost", v2="false").cursor()

        def handler(method, path, payload):
            if path.startswith("/_plugins/"):
                return ODFE_PLUGINS_SQL
            return 200, {
                "schema": [
                    {"name": "v", "alias": "k__es0", "type": "integer"},
                    {"name": "k", "alias": "key", "type": "keyword"},
                ],
                "datarows": [[0, "k449"]],
                "total": 1,
                "size": 1,
                "status": 200,
            }

        with FakeCluster(cursor, handler) as cluster:
            cursor.execute(
                "SELECT v AS k, grp.k AS key FROM grp ORDER BY grp.k DESC LIMIT 1"
            )
        sent = cluster.sql_requests()[-1][2]
        self.assertIn("fetch_size", sent)
        self.assertTrue(sent["query"].startswith("SELECT v AS k__es0,"))
        self.assertEqual([d[0] for d in cursor.description], ["k", "key"])
