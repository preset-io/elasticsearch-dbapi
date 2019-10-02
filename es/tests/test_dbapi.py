import unittest

from es.elastic.api import Type, connect
from es.exceptions import OperationalError, ProgrammingError


class TestData(unittest.TestCase):
    def setUp(self):
        self.conn = connect(host="localhost")
        self.cursor = self.conn.cursor()

    def test_connect_failed(self):
        """
        DBAPI: Test connection failed
        """
        conn = connect(host="unknown")
        curs = conn.cursor()
        with self.assertRaises(OperationalError):
            curs.execute("select Carrier from flights").fetchall()

    def test_execute_fetchall(self):
        """
        DBAPI: Test execute and fetchall
        """
        rows = self.cursor.execute("select Carrier from flights").fetchall()
        self.assertGreater(len(rows), 1)

    def test_execute_fetchone(self):
        """
        DBAPI: Test execute and fectchone
        """
        rows = self.cursor.execute("select Carrier from flights").fetchone()
        self.assertEquals(len(rows), 1)

    def test_execute_rowcount(self):
        """
        DBAPI: Test execute and rowcount
        """
        count = self.cursor.execute("select Carrier from flights LIMIT 10").rowcount
        self.assertEquals(count, 10)

    def test_execute_wrong_table(self):
        """
        DBAPI: Test execute select with wrong table
        """
        with self.assertRaises(ProgrammingError):
            self.cursor.execute("select Carrier from no_table LIMIT 10").rowcount

    def test_execute_select_all(self):
        """
        DBAPI: Test execute select all (*)
        """
        rows = self.cursor.execute("select * from flights LIMIT 10").fetchall()
        self.assertEquals(len(rows), 10)

    def test_boolean_description(self):
        """
        DBAPI: Test boolean description
        """
        rows = self.cursor.execute("select Cancelled from flights LIMIT 1")
        self.assertEquals(
            rows.description,
            [("Cancelled", Type.BOOLEAN, None, None, None, None, True)],
        )

    def test_number_description(self):
        """
        DBAPI: Test number description
        """
        rows = self.cursor.execute("select FlightDelayMin from flights LIMIT 1")
        self.assertEquals(
            rows.description,
            [("FlightDelayMin", Type.NUMBER, None, None, None, None, True)],
        )

    def test_string_description(self):
        """
        DBAPI: Test string description
        """
        rows = self.cursor.execute("select DestCountry from flights LIMIT 1")
        self.assertEquals(
            rows.description,
            [("DestCountry", Type.STRING, None, None, None, None, True)],
        )

    def test_datetime_description(self):
        """
        DBAPI: Test datetime description
        """
        rows = self.cursor.execute("select timestamp from flights LIMIT 1")
        self.assertEquals(
            rows.description,
            [("timestamp", Type.DATETIME, None, None, None, None, True)],
        )

    def test_simple_group_by(self):
        """
        DBAPI: Test simple group by
        """
        rows = self.cursor.execute(
            "select COUNT(*) as c, Carrier from flights GROUP BY Carrier"
        ).fetchall()
        # poor assertion because that is loaded async
        self.assertGreater(len(rows), 1)
