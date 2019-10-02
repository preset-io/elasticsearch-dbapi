import unittest

from es.exceptions import OperationalError, ProgrammingError
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData


class TestData(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("es+http://localhost:9200/")
        self.connection = self.engine.connect()

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
        self.assertEqual(tables, ["flights"])
