import os
import unittest
from unittest.mock import patch

from es.elastic.api import connect as elastic_connect, Type
from es.exceptions import Error, NotSupportedError, OperationalError, ProgrammingError
from es.opendistro.api import connect as open_connect


class TestDBAPI(unittest.TestCase):
    def setUp(self):
        self.driver_name = os.environ.get("ES_DRIVER", "elasticsearch")
        host = os.environ.get("ES_HOST", "localhost")
        port = int(os.environ.get("ES_PORT", 9200))
        scheme = os.environ.get("ES_SCHEME", "http")
        verify_certs = os.environ.get("ES_VERIFY_CERTS", False)
        user = os.environ.get("ES_USER", None)
        password = os.environ.get("ES_PASSWORD", None)
        self.v2 = bool(os.environ.get("ES_V2", False))

        if self.driver_name == "elasticsearch":
            self.connect_func = elastic_connect
        else:
            self.connect_func = open_connect
        self.conn = self.connect_func(
            host=host,
            port=port,
            scheme=scheme,
            verify_certs=verify_certs,
            user=user,
            password=password,
            v2=self.v2,
        )
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_connect_failed(self):
        """
        DBAPI: Test connection failed
        """
        conn = self.connect_func(host="unknown")
        curs = conn.cursor()
        with self.assertRaises(OperationalError):
            curs.execute("select Carrier from flights").fetchall()
        conn.close()

    def test_close(self):
        """
        DBAPI: Test connection failed
        """
        conn = self.connect_func(host="localhost")
        conn.close()
        with self.assertRaises(Error):
            conn.close()

    def test_execute_fetchall(self):
        """
        DBAPI: Test execute and fetchall
        """
        rows = self.cursor.execute("select Carrier from flights").fetchall()
        self.assertEqual(len(rows), 31)

    def test_execute_on_connect(self):
        """
        DBAPI: Test execute, fetchall on connect
        """
        rows = self.conn.execute("select Carrier from flights").fetchall()
        self.assertEqual(len(rows), 31)

    def test_commit_executes(self):
        """
        DBAPI: Test commit method exists
        """
        self.conn.commit()

    def test_executemany_not_supported(self):
        """
        DBAPI: Test executemany not supported
        """
        with self.assertRaises(NotSupportedError):
            self.cursor.executemany("select Carrier from flights")

    def test_execute_fetchmany(self):
        """
        DBAPI: Test execute and fectchmany
        """
        rows = self.cursor.execute("select Carrier from flights").fetchmany(2)
        self.assertEquals(len(rows), 2)

    def test_execute_fetchone(self):
        """
        DBAPI: Test execute and fectchone
        """
        rows = self.cursor.execute("select Carrier from flights").fetchone()
        self.assertEquals(len(rows), 1)

    def test_execute_empty_results(self):
        """
        DBAPI: Test execute query with no results
        """
        rows = self.cursor.execute(
            "select Carrier from flights where Carrier='NORESULT'"
        ).fetchall()
        self.assertEquals(len(rows), 0)

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
        # Make sure we have a list of tuples
        self.assertEqual(type(rows), type(list()))
        self.assertEqual(type(rows[0]), type(tuple()))
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
        if self.v2:
            group_by_column = "Carrier"
        else:
            group_by_column = "Carrier.keyword"
        rows = self.cursor.execute(
            f"select COUNT(*) as c, {group_by_column} "
            f"from flights GROUP BY {group_by_column}"
        ).fetchall()
        # poor assertion because that is loaded async
        self.assertEqual(len(rows), 4)

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_auth(self, mock_elasticsearch):
        """
        DBAPI: test Elasticsearch is called with user password
        """
        mock_elasticsearch.return_value = None
        self.connect_func(
            host="localhost", scheme="http", port=9200, user="user", password="password"
        )
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200/", http_auth=("user", "password")
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_https(self, mock_elasticsearch):
        """
        DBAPI: test Elasticsearch is called with https
        """
        mock_elasticsearch.return_value = None
        self.connect_func(
            host="localhost",
            user="user",
            password="password",
            scheme="https",
            port=9200,
        )
        mock_elasticsearch.assert_called_once_with(
            "https://localhost:9200/", http_auth=("user", "password")
        )
