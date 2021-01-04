import os
import unittest

from .fixtures.fixtures import delete_index, import_data1, import_flights

BASE_URL = "http://localhost:9200"


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("ES_URI", BASE_URL)

    def test_data_flights(self):
        delete_index(self.base_url, "flights")
        import_flights(self.base_url)

    def test_data_data1(self):
        delete_index(self.base_url, "data1")
        import_data1(self.base_url)
