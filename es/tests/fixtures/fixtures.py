import json
import os

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


flights_columns = [
    "AvgTicketPrice",
    "Cancelled",
    "Carrier",
    "Carrier.keyword",
    "Dest",
    "Dest.keyword",
    "DestAirportID",
    "DestAirportID.keyword",
    "DestCityName",
    "DestCityName.keyword",
    "DestCountry",
    "DestCountry.keyword",
    "DestLocation.lat",
    "DestLocation.lat.keyword",
    "DestLocation.lon",
    "DestLocation.lon.keyword",
    "DestRegion",
    "DestRegion.keyword",
    "DestWeather",
    "DestWeather.keyword",
    "DistanceKilometers",
    "DistanceMiles",
    "FlightDelay",
    "FlightDelayMin",
    "FlightDelayType",
    "FlightDelayType.keyword",
    "FlightNum",
    "FlightNum.keyword",
    "FlightTimeHour",
    "FlightTimeMin",
    "Origin",
    "Origin.keyword",
    "OriginAirportID",
    "OriginAirportID.keyword",
    "OriginCityName",
    "OriginCityName.keyword",
    "OriginCountry",
    "OriginCountry.keyword",
    "OriginLocation.lat",
    "OriginLocation.lat.keyword",
    "OriginLocation.lon",
    "OriginLocation.lon.keyword",
    "OriginRegion",
    "OriginRegion.keyword",
    "OriginWeather",
    "OriginWeather.keyword",
    "dayOfWeek",
    "timestamp",
]

data1_columns = [
    "field_boolean",
    "field_float",
    "field_nested.c1",
    "field_nested.c1.keyword",
    "field_nested.c2",
    "field_number",
    "field_str",
    "field_str.keyword",
    "timestamp",
]


def import_file_to_es(base_url, file_path, index_name):

    fd = open(file_path, "r")
    data = json.load(fd)
    fd.close()

    set_index_replica_zero(base_url, index_name)
    es = Elasticsearch(base_url)
    for doc in data:
        es.index(index=index_name, doc_type="_doc", body=doc, refresh=True)


def set_index_replica_zero(base_url, index_name):
    settings = {"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
    es = Elasticsearch(base_url)
    es.indices.create(index=index_name, ignore=400, body=settings)


def delete_index(base_url, index_name):
    es = Elasticsearch(base_url)
    try:
        es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
    except NotFoundError:
        return


def import_flights(base_url):
    path = os.path.join(os.path.dirname(__file__), "flights.json")
    import_file_to_es(base_url, path, "flights")


def import_data1(base_url):
    path = os.path.join(os.path.dirname(__file__), "data1.json")
    import_file_to_es(base_url, path, "data1")
