import os
import unittest
from unittest.mock import patch

from es.tests.fixtures.fixtures import data1_columns, flights_columns
from sqlalchemy import func, inspect, select
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import MetaData, Table


class TestData(unittest.TestCase):
    def setUp(self):
        self.driver_name = os.environ.get("ES_DRIVER", "elasticsearch")
        host = os.environ.get("ES_HOST", "localhost")
        port = int(os.environ.get("ES_PORT", 9200))
        scheme = os.environ.get("ES_SCHEME", "http")
        verify_certs = os.environ.get("ES_VERIFY_CERTS", "False")
        user = os.environ.get("ES_USER", None)
        password = os.environ.get("ES_PASSWORD", None)

        uri = URL(
            f"{self.driver_name}+{scheme}",
            user,
            password,
            host,
            port,
            None,
            {"verify_certs": str(verify_certs)},
        )
        self.engine = create_engine(uri)
        self.connection = self.engine.connect()
        self.table_flights = Table("flights", MetaData(bind=self.engine), autoload=True)

    def test_simple_query(self):
        """
        SQLAlchemy: Test simple query
        """
        rows = self.connection.execute("select Carrier from flights").fetchall()
        self.assertGreater(len(rows), 1)

    def test_execute_wrong_table(self):
        """
        SQLAlchemy: Test execute select with wrong table
        """
        with self.assertRaises(ProgrammingError):
            self.connection.execute("select Carrier from no_table LIMIT 10").fetchall()

    def test_reflection_get_tables(self):
        """
        SQLAlchemy: Test reflection get_tables
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = [str(table) for table in metadata.sorted_tables]
        self.assertIn("flights", tables)

    def test_has_table(self):
        """
        SQLAlchemy: Test has_table
        """
        self.assertTrue(self.engine.has_table("flights"))

    def test_get_schema_names(self):
        """
        SQLAlchemy: Test get schema names
        """
        insp = inspect(self.engine)
        self.assertEqual(insp.get_schema_names(), ["default"])

    def test_reflection_get_columns(self):
        """
        SQLAlchemy: Test get_columns
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        source_cols = [c.name for c in metadata.tables["flights"].c]
        self.assertEqual(flights_columns, source_cols)

    def test_get_columns_exclude_arrays(self):
        """
        SQLAlchemy: Test get_columns exclude arrays (elastic only)
        """
        if self.driver_name == "odelasticsearch":
            return
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        source_cols = [c.name for c in metadata.tables["data1"].c]
        self.assertEqual(data1_columns, source_cols)

    def test_get_columns_exclude_geo_point(self):
        """
        SQLAlchemy: Test get_columns exclude geo point (odelasticsearch only)
        """
        if self.driver_name == "elasticsearch":
            return
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        source_cols = [c.name for c in metadata.tables["data1"].c]
        expected_columns = [
            "field_boolean",
            "field_float",
            "field_nested.c1",
            "field_nested.c1.keyword",
            "field_nested.c2",
            "field_number",
            "field_str",
            "field_str.keyword",
            "timestamp",
        ]
        self.assertEqual(source_cols, expected_columns)

    def test_select_count(self):
        """
        SQLAlchemy: Test select all
        """
        count = select([func.count("*")], from_obj=self.table_flights).scalar()
        # insert data delays let's assert we have something there
        self.assertGreater(count, 1)

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_auth(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with user password
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine(
            "elasticsearch+http://user:password@localhost:9200/"
        )
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200/", http_auth=("user", "password")
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_connection_https_and_auth(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with https and param
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine(
            "elasticsearch+https://user:password@localhost:9200/"
        )
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "https://localhost:9200/", http_auth=("user", "password")
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_connection_https_and_params(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with https and param
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine(
            "elasticsearch+https://localhost:9200/"
            "?verify_certs=False"
            "&use_ssl=False"
        )
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "https://localhost:9200/", verify_certs=False, use_ssl=False
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_connection_params(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with advanced config params
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine(
            "elasticsearch+http://localhost:9200/"
            "?http_compress=True&maxsize=100&timeout=3"
        )
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200/", http_compress=True, maxsize=100, timeout=3
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_connection_params_value_error(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch with param value error
        """
        mock_elasticsearch.return_value = None
        with self.assertRaises(ValueError):
            self.engine = create_engine(
                "elasticsearch+http://localhost:9200/" "?http_compress=cena"
            )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_connection_sniff(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called for multiple hosts
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine(
            "elasticsearch+http://localhost:9200/"
            "?sniff_on_start=True"
            "&sniff_on_connection_fail=True"
            "&sniffer_timeout=3"
            "&sniff_timeout=4"
            "&max_retries=10"
            "&retry_on_timeout=True"
        )
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200/",
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=3,
            sniff_timeout=4,
            max_retries=10,
            retry_on_timeout=True,
        )
