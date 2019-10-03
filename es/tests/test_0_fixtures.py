import unittest

from .fixtures.fixtures import delete_all, import_flights

BASE_URL = "http://localhost:9200"


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = f"{BASE_URL}"
        # Yes, it will delete all indexes!
        delete_all(self.base_url)

    def test_data_flights(self):
        import_flights(self.base_url)
