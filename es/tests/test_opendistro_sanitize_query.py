import unittest
from unittest.mock import Mock

from es.opendistro.api import Cursor


class TestSanitizeQuery(unittest.TestCase):
    """Unit tests for Cursor.sanitize_query — no live cluster required."""

    def _sanitize(self, query: str) -> str:
        return Cursor.sanitize_query(Mock(spec=Cursor), query)

    def test_strips_double_quoted_default_schema(self):
        self.assertEqual(
            self._sanitize('SELECT a FROM "default".flights'),
            "SELECT a FROM flights",
        )

    def test_strips_backtick_quoted_default_schema(self):
        self.assertEqual(
            self._sanitize("SELECT a FROM `default`.flights"),
            "SELECT a FROM flights",
        )

    def test_no_match_is_unchanged(self):
        query = "SELECT a FROM flights WHERE Carrier = 'Kibana Airlines'"
        self.assertEqual(self._sanitize(query), query)

    def test_does_not_strip_unrelated_quoted_default_token(self):
        query = "SELECT 'default' AS label FROM flights"
        self.assertEqual(self._sanitize(query), query)

    def test_does_not_strip_identifier_delimiters(self):
        query = "SELECT count(*) as `my test` from foo"
        self.assertEqual(self._sanitize(query), query)
