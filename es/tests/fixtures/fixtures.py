import json
import os
from typing import Any, Dict, Optional

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
    "location",
    "timestamp",
]


def import_file_to_es(
    base_url: str, data_path: str, index_name: str, mappings_path: Optional[str] = None
) -> None:

    with open(data_path, "r") as fd_data:
        data = json.load(fd_data)

    mappings = None
    if mappings_path:
        with open(mappings_path, "r") as fd_mappings:
            mappings = json.load(fd_mappings)

    set_index_settings(base_url, index_name, mappings=mappings)
    es = Elasticsearch(base_url, verify_certs=False)
    for doc in data:
        es.index(index=index_name, doc_type="_doc", body=doc, refresh=True)


def set_index_settings(
    base_url: str, index_name: str, mappings: Optional[Dict[str, Any]] = None
) -> None:
    """
    Sets index settings for number of replicas to ZERO by default, and applies optional
    mappings
    """
    body = {"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
    if mappings:
        body.update(mappings)
    es = Elasticsearch(base_url, verify_certs=False)
    es.indices.create(index=index_name, ignore=400, body=body)


def delete_index(base_url, index_name: str) -> None:
    es = Elasticsearch(base_url, verify_certs=False)
    try:
        es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
    except NotFoundError:
        return


def delete_alias(base_url, alias_name: str, index_name: str) -> None:
    es = Elasticsearch(base_url, verify_certs=False)
    try:
        es.indices.delete_alias(index=index_name, name=alias_name)
    except NotFoundError:
        return


def create_alias(base_url, alias_name: str, index_name: str) -> None:
    es = Elasticsearch(base_url, verify_certs=False)
    try:
        es.indices.put_alias(index=index_name, name=alias_name)
    except NotFoundError:
        return


def import_flights(base_url: str) -> None:
    path = os.path.join(os.path.dirname(__file__), "flights.json")
    import_file_to_es(base_url, path, "flights")


def import_data1(base_url: str) -> None:
    data_path = os.path.join(os.path.dirname(__file__), "data1.json")
    mappings_path = os.path.join(os.path.dirname(__file__), "data1_mappings.json")

    import_file_to_es(base_url, data_path, "data1", mappings_path=mappings_path)


def import_empty_index(base_url):
    set_index_settings(base_url, "empty_index")
