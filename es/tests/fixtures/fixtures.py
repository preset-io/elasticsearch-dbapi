import json
import os
from typing import Any, Dict, Optional, Union

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError as ESNotFoundError
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError as OSNotFoundError
from opensearchpy.exceptions import RequestError as OSRequestError


def get_client(base_url: str) -> Union[Elasticsearch, OpenSearch]:
    """
    Returns the appropriate client based on ES_DRIVER environment variable.
    Uses OpenSearch client for 'odelasticsearch' driver, Elasticsearch otherwise.
    """
    driver = os.environ.get("ES_DRIVER", "elasticsearch")
    if driver == "odelasticsearch":
        return OpenSearch(base_url, verify_certs=False, use_ssl=False)
    return Elasticsearch(base_url, verify_certs=False)


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
    client = get_client(base_url)
    driver = os.environ.get("ES_DRIVER", "elasticsearch")
    for doc in data:
        if driver == "odelasticsearch":
            client.index(index=index_name, body=doc, refresh=True)
        else:
            client.index(index=index_name, document=doc, refresh=True)


def set_index_settings(
    base_url: str, index_name: str, mappings: Optional[Dict[str, Any]] = None
) -> None:
    """
    Sets index settings for number of replicas to ZERO by default, and applies optional
    mappings
    """
    settings = {"number_of_shards": 1, "number_of_replicas": 0}
    client = get_client(base_url)
    driver = os.environ.get("ES_DRIVER", "elasticsearch")
    body_mappings = mappings.get("mappings") if mappings else None
    if driver == "odelasticsearch":
        # OpenSearch uses body parameter
        body: Dict[str, Any] = {"settings": settings}
        if body_mappings:
            body["mappings"] = body_mappings
        try:
            client.indices.create(index=index_name, body=body)
        except OSRequestError:
            pass  # Index already exists
    else:
        # ES 8.x: use options() to ignore errors and separate params
        client.options(ignore_status=400).indices.create(  # type: ignore[union-attr]
            index=index_name,
            settings=settings,
            mappings=body_mappings,
        )


def delete_index(base_url: str, index_name: str) -> None:
    client = get_client(base_url)
    driver = os.environ.get("ES_DRIVER", "elasticsearch")
    try:
        if driver == "odelasticsearch":
            client.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
        else:
            client.delete_by_query(index=index_name, query={"match_all": {}})
    except (ESNotFoundError, OSNotFoundError):
        return


def delete_alias(base_url: str, alias_name: str, index_name: str) -> None:
    client = get_client(base_url)
    try:
        client.indices.delete_alias(index=index_name, name=alias_name)
    except (ESNotFoundError, OSNotFoundError):
        return


def create_alias(base_url: str, alias_name: str, index_name: str) -> None:
    client = get_client(base_url)
    try:
        client.indices.put_alias(index=index_name, name=alias_name)
    except (ESNotFoundError, OSNotFoundError):
        return


def import_flights(base_url: str) -> None:
    path = os.path.join(os.path.dirname(__file__), "flights.json")
    import_file_to_es(base_url, path, "flights")


def import_data1(base_url: str) -> None:
    data_path = os.path.join(os.path.dirname(__file__), "data1.json")
    mappings_path = os.path.join(os.path.dirname(__file__), "data1_mappings.json")

    import_file_to_es(base_url, data_path, "data1", mappings_path=mappings_path)


def import_empty_index(base_url: str) -> None:
    set_index_settings(base_url, "empty_index")
