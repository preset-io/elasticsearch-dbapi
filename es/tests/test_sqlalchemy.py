import unittest
from unittest.mock import patch

from es.tests.fixtures.fixtures import flights_columns

from sqlalchemy import func, select
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.schema import MetaData, Table


class TestData(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("es+http://localhost:9200/")
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

    def test_get_tables(self):
        """
        SQLAlchemy: Test get_tables
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = [str(table) for table in metadata.sorted_tables]
        self.assertIn("flights", tables)

    def test_get_columns(self):
        """
        SQLAlchemy: Test get_columns
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        source_cols = [c.name for c in metadata.tables["flights"].c]
        self.assertEqual(flights_columns, source_cols)

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
        self.engine = create_engine("es+http://user:password@localhost:9200/")
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200", http_auth=("user", "password"),
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_https_and_params(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with https and param
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine("es+https://user:password@localhost:9200/?param=a")
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "https://localhost:9200", http_auth=("user", "password"), param="a",
        )
