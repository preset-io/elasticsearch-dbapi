"""
Unit tests for Elasticsearch SQL cursor pagination in ``es.elastic.api``.

Unlike the rest of ``es/tests``, these tests mock the transport layer
instead of requiring a live Elasticsearch/OpenSearch cluster, so they can
run fast and standalone. They exist specifically to pin down the
cursor-following behavior added to ``Cursor.execute``: previously, only
the first page of a query's results (bounded by ``fetch_size``) was ever
returned, silently dropping any additional rows.
"""

import unittest
from unittest.mock import patch

from es.elastic.api import connect


class TestCursorPagination(unittest.TestCase):
    def setUp(self):
        self.conn = connect(host="localhost")
        self.cursor = self.conn.cursor()

    def test_execute_follows_cursor_across_pages(self):
        """
        A response that includes a ``cursor`` field must not stop after
        the first page: subsequent pages should be fetched and their rows
        appended, until a response with no ``cursor`` is reached.
        """
        responses = [
            {
                "columns": [{"name": "a", "type": "long"}],
                "rows": [[1], [2]],
                "cursor": "page-2-cursor",
            },
            {"rows": [[3], [4]], "cursor": "page-3-cursor"},
            {"rows": [[5]]},  # final page: no cursor
        ]

        with patch.object(
            self.cursor.es.transport, "perform_request", side_effect=responses
        ) as mock_request:
            self.cursor.execute("select a from some_table")

        self.assertEqual(self.cursor.fetchall(), [(1,), (2,), (3,), (4,), (5,)])
        self.assertEqual(mock_request.call_count, 3)

        # first call is the initial query; the rest follow the cursor
        first_call = mock_request.call_args_list[0]
        self.assertEqual(first_call.args[0], "POST")
        self.assertIn("query", first_call.kwargs["body"])

        second_call = mock_request.call_args_list[1]
        self.assertEqual(second_call.kwargs["body"], {"cursor": "page-2-cursor"})

        third_call = mock_request.call_args_list[2]
        self.assertEqual(third_call.kwargs["body"], {"cursor": "page-3-cursor"})

    def test_execute_single_page_unaffected(self):
        """
        A response with no ``cursor`` at all (result fits in one page)
        should behave exactly as before: a single request, no pagination
        follow-up, no close call.
        """
        responses = [
            {"columns": [{"name": "a", "type": "long"}], "rows": [[1], [2]]},
        ]

        with patch.object(
            self.cursor.es.transport, "perform_request", side_effect=responses
        ) as mock_request:
            self.cursor.execute("select a from some_table")

        self.assertEqual(self.cursor.fetchall(), [(1,), (2,)])
        self.assertEqual(mock_request.call_count, 1)

    def test_cursor_closed_on_mid_pagination_error(self):
        """
        If a follow-up page request raises mid-pagination, the still-open
        cursor must be closed (best-effort) rather than leaked, and the
        original exception should still propagate.
        """
        from elasticsearch import exceptions as es_exceptions

        first_response = {
            "columns": [{"name": "a", "type": "long"}],
            "rows": [[1]],
            "cursor": "page-2-cursor",
        }

        def side_effect(method, path, **kwargs):
            if path.endswith("/close"):
                return {}
            if kwargs.get("body", {}).get("cursor") == "page-2-cursor":
                raise es_exceptions.ConnectionError("boom")
            return first_response

        with patch.object(
            self.cursor.es.transport, "perform_request", side_effect=side_effect
        ) as mock_request:
            from es.exceptions import OperationalError

            with self.assertRaises(OperationalError):
                self.cursor.execute("select a from some_table")

        close_calls = [
            call
            for call in mock_request.call_args_list
            if call.args[1].endswith("/close")
        ]
        self.assertEqual(len(close_calls), 1)
        self.assertEqual(close_calls[0].kwargs["body"], {"cursor": "page-2-cursor"})


if __name__ == "__main__":
    unittest.main()
