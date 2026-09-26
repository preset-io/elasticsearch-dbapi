"""
Offline regression tests for SQLAlchemy 2 / DB-API contract fixes.

They mock the transport layer or only compile statements, so they run
without a cluster, in every CI job.
"""

import datetime
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

DIALECTS = (ESDialect, ESHTTPSDialect, ODDialect, ODHTTPSDialect)


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

    def test_v2_string_false_is_false_and_fetch_size_is_kept(self):
        self.assertFalse(self.cursor(v2="False").v2)
        v2 = self.cursor(v2="true")
        self.assertTrue(v2.v2)
        self.assertIsNotNone(v2.fetch_size)

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

    def test_time_zone_is_refused(self):
        with self.assertRaises(exceptions.NotSupportedError):
            opendistro_api.connect(host="localhost", time_zone="+02:00")

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

    def _fallback(self, unpaged_rows, size_limit):
        cursor = self.cursor()
        failure = os_exceptions.TransportError(500, "IllegalStateException", {})
        settings = (
            {"defaults": {"plugins.query.size_limit": str(size_limit)}}
            if size_limit is not None
            else RuntimeError("forbidden")
        )

        def perform_request(method, path, **kwargs):
            if path == "/_cluster/settings":
                if isinstance(settings, Exception):
                    raise settings
                return settings
            if "fetch_size" in kwargs["body"]:
                raise failure
            return {
                "schema": [{"name": "w", "type": "text"}],
                "datarows": [[str(i)] for i in range(unpaged_rows)],
            }

        with patch.object(
            cursor.es.transport, "perform_request", side_effect=perform_request
        ):
            return cursor.execute("select w, count(*) from t group by w").fetchall()

    def test_unpaged_retry_accepted_below_the_size_limit(self):
        self.assertEqual(len(self._fallback(5, 10)), 5)

    def test_unpaged_retry_refused_when_it_may_be_truncated(self):
        with self.assertRaises(exceptions.DataError):
            self._fallback(10, 10)

    def test_original_error_when_size_limit_is_unreadable(self):
        with self.assertRaises(exceptions.DatabaseError) as ctx:
            self._fallback(5, None)
        self.assertNotIsInstance(ctx.exception, exceptions.DataError)


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

    def test_correlated_references_keep_their_qualifier(self):
        outer = sa.table("t1", sa.column("x"))
        inner = sa.table("t2", sa.column("x"))
        exists = sa.exists().where(inner.c.x == outer.c.x)
        sql = str(sa.select(outer.c.x).where(exists).compile(dialect=ODDialect()))
        self.assertIn("WHERE x = t1.x", sql.split("EXISTS")[1])


if __name__ == "__main__":
    unittest.main()
