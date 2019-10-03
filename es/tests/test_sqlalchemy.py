import unittest
from unittest.mock import patch

from es.exceptions import ProgrammingError
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.exc import ProgrammingError


class TestData(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("es+http://localhost:9200/")
        self.connection = self.engine.connect()

    def test_simple_query(self):
        """
        SQLAlchemy: Test simple query
        """
        rows = self.connection.execute("select Carrier from flights").fetchall()
        self.assertGreater(len(rows), 1)

    def test_execute_wrong_table(self):
        """
        SQLAlchemy: Test execute select with wrong table
        """
        with self.assertRaises(ProgrammingError):
            self.connection.execute("select Carrier from no_table LIMIT 10").fetchall()

    def test_get_tables(self):
        """
        SQLAlchemy: Test get_tables
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = [str(table) for table in metadata.sorted_tables]
        self.assertEqual(tables, ["flights"])

    def test_get_columns(self):
        """
        SQLAlchemy: Test get_columns
        """
        table = Table("flights", MetaData(bind=self.engine), autoload=True)
        #raise Exception(table.columns)
        for col in table.columns:
            print(col)
        raise Exception()
        self.assertEqual(list(table.columns), ['flights.AvgTicketPrice', 'flights.Cancelled', 'flights.Carrier', 'flights.Carrier.keyword', 'flights.Dest', 'flights.Dest.keyword', 'flights.DestAirportID', 'flights.DestAirportID.keyword', 'flights.DestCityName', 'flights.DestCityName.keyword', 'flights.DestCountry', 'flights.DestCountry.keyword', 'flights.DestLocation.lat', 'flights.DestLocation.lat.keyword', 'flights.DestLocation.lon', 'flights.DestLocation.lon.keyword', 'flights.DestRegion', 'flights.DestRegion.keyword', 'flights.DestWeather', 'flights.DestWeather.keyword', 'flights.DistanceKilometers', 'flights.DistanceMiles', 'flights.FlightDelay', 'flights.FlightDelayMin', 'flights.FlightDelayType', 'flights.FlightDelayType.keyword', 'flights.FlightNum', 'flights.FlightNum.keyword', 'flights.FlightTimeHour', 'flights.FlightTimeMin', 'flights.Origin', 'flights.Origin.keyword', 'flights.OriginAirportID', 'flights.OriginAirportID.keyword', 'flights.OriginCityName', 'flights.OriginCityName.keyword', 'flights.OriginCountry', 'flights.OriginCountry.keyword', 'flights.OriginLocation.lat', 'flights.OriginLocation.lat.keyword', 'flights.OriginLocation.lon', 'flights.OriginLocation.lon.keyword', 'flights.OriginRegion', 'flights.OriginRegion.keyword', 'flights.OriginWeather', 'flights.OriginWeather.keyword', 'flights.dayOfWeek', 'flights.timestamp'])

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_auth(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with user password
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine("es+http://user:password@localhost:9200/")
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "http://localhost:9200", http_auth=('user', 'password')
        )

    @patch("elasticsearch.Elasticsearch.__init__")
    def test_https_and_params(self, mock_elasticsearch):
        """
            SQLAlchemy: test Elasticsearch is called with https and param
        """
        mock_elasticsearch.return_value = None
        self.engine = create_engine("es+https://user:password@localhost:9200/?param=a")
        self.connection = self.engine.connect()
        mock_elasticsearch.assert_called_once_with(
            "https://localhost:9200", http_auth=('user', 'password'), param='a',
        )
