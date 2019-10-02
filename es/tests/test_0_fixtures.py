import unittest

from .fixtures.fixtures import import_flights

BASE_URL = "http://localhost:9200"
BASE_URL = "https://search-test-vfxugcjhw2bnqysr4zgogrxwa4.us-west-2.es.amazonaws.com:443"


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = f"{BASE_URL}"

    def test_data_flights(self):
        import_flights(self.base_url)
