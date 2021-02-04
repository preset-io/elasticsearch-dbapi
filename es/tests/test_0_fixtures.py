import os
import unittest

from .fixtures.fixtures import (
    create_alias,
    delete_alias,
    delete_index,
    import_data1,
    import_empty_index,
    import_flights,
)

BASE_URL = "http://localhost:9200"


class TestData(unittest.TestCase):
    def setUp(self):
        self.base_url = os.environ.get("ES_URI", BASE_URL)

    def test_1_data_flights(self):
        delete_index(self.base_url, "flights")
        import_flights(self.base_url)

    def test_2_data_data1(self):
        delete_index(self.base_url, "data1")
        import_data1(self.base_url)

    def test_3_data_empty_index(self):
        delete_index(self.base_url, "empty_index")
        import_empty_index(self.base_url)

    def test_4_alias_to_data1(self):
        alias_name = "alias_to_data1"
        delete_alias(self.base_url, alias_name, "data1")
        create_alias(self.base_url, alias_name, "data1")
