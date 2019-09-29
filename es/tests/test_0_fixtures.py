import os
import unittest

from .fixtures.fixtures import import_file_to_es


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = "http://localhost:9200"

    def test_data_logs(self):
        path = os.path.join(os.path.dirname(__file__), "fixtures/logs.json")
        import_file_to_es(self.base_url, path, "logs")
