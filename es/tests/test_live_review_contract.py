"""Live regressions for the SQL plugin contract (run with ES_DRIVER=odelasticsearch)."""

import datetime
import os
import uuid
from urllib.parse import urlsplit

import pytest
from opensearchpy import OpenSearch
from sqlalchemy import column, create_engine, func, select, table

from es.baseapi import Type
from es.opendistro.api import connect


@pytest.fixture(scope="module")
def review_index():
    if os.environ.get("ES_DRIVER") != "odelasticsearch":
        pytest.skip("requires the OpenSearch SQL plugin")
    url = os.environ.get("ES_URI", "http://localhost:9200")
    client = OpenSearch(url)
    index = "review-contract-" + uuid.uuid4().hex
    try:
        client.indices.create(
            index=index,
            body={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "properties": {
                        "k": {"type": "keyword"},
                        "sm": {"type": "integer"},
                        "v": {"type": "integer"},
                        "ts": {"type": "date"},
                    }
                },
            },
        )
        body = []
        for i in range(12000):
            body.extend(
                [
                    {"index": {"_index": index}},
                    {
                        "k": f"key{i:05d}",
                        "sm": i % 28 + 1,
                        "v": i,
                        "ts": "2024-01-01T00:30:00Z",
                    },
                ]
            )
        response = client.bulk(body=body, refresh=True)
        assert not response["errors"]
        yield url, index
    finally:
        client.indices.delete(index=index, ignore=404)
        client.close()


def connection(url, v2):
    parsed = urlsplit(url)
    return connect(host=parsed.hostname, port=parsed.port, scheme=parsed.scheme, v2=v2)


def test_v2_limit_keeps_function_and_timestamp_types(review_index):
    url, index = review_index
    engine = create_engine(
        url.replace("http://", "odelasticsearch+http://") + "?v2=true"
    )
    try:
        with engine.connect() as conn:
            source = table(index, column("k"))
            rows = conn.execute(
                select(func.concat(source.c.k, "x")).limit(5)
            ).fetchall()
            assert len(rows) == 5
            assert all(row[0].endswith("x") for row in rows)
    finally:
        engine.dispose()
    conn = connection(url, True)
    try:
        for limit in ("", " LIMIT 5"):
            cursor = conn.cursor().execute(
                f"SELECT ts FROM `{index}` WHERE v < 5{limit}"
            )
            assert cursor.description[0].type == Type.DATETIME
            assert cursor.fetchall() == [(datetime.datetime(2024, 1, 1, 0, 30),)] * 5
    finally:
        conn.close()
