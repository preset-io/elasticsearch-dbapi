import unittest

from es.api import connect
from es.exceptions import OperationalError, ProgrammingError


class TestData(unittest.TestCase):
    def setUp(self):
        self.conn = connect(host='localhost')
        self.cursor = self.conn.cursor()

    def test_connect_failed(self):
        conn = connect(host='unknown')
        curs = conn.cursor()
        with self.assertRaises(OperationalError):
            curs.execute("select agent from logs").fetchall()

    def test_execute_fetchall(self):
        rows = self.cursor.execute("select agent from logs").fetchall()
        self.assertGreater(len(rows), 1)

    def test_execute_fetchone(self):
        rows = self.cursor.execute("select ip from logs").fetchone()
        self.assertEquals(len(rows), 1)

    def test_execute_rowcount(self):
        count = self.cursor.execute("select agent from logs LIMIT 10").rowcount
        self.assertEquals(count, 10)

    def test_execute_wrong_table(self):
        with self.assertRaises(ProgrammingError):
            self.cursor.execute("select agent from no_table LIMIT 10").rowcount
