import unittest

from .fixtures.fixtures import import_flights


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = "http://localhost:9200"

    def test_data_flights(self):
        import_flights(self.base_url)
