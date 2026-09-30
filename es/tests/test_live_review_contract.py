"""Live regressions for the SQL plugin contract (run with ES_DRIVER=odelasticsearch)."""

import datetime
import os
from urllib.parse import urlsplit
import uuid

from es.baseapi import Type
from es.opendistro.api import connect
from opensearchpy import OpenSearch
import pytest
from sqlalchemy import column, create_engine, func, select, table


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


def test_legacy_grouping_alias_and_having(review_index):
    url, index = review_index
    conn = connection(url, False)
    query = (
        f"SELECT floor(evt.sm / 10) AS sm, COUNT(*) AS c FROM `{index}` evt "
        "WHERE evt.sm > 0 AND evt.v < 56 GROUP BY sm"
    )
    try:
        cursor = conn.cursor().execute(query)
        assert [col.name for col in cursor.description] == ["sm", "c"]
        assert sorted((float(k), c) for k, c in cursor.fetchall()) == [
            (0, 18),
            (1, 20),
            (2, 18),
        ]
        having_alias = (
            conn.cursor()
            .execute(
                f"SELECT k, COUNT(*) AS v FROM `{index}` evt "
                "WHERE evt.v < 5 GROUP BY k HAVING v > 0"
            )
            .fetchall()
        )
        assert sorted(having_alias) == [(f"key{i:05d}", 1) for i in range(5)]
        from es.exceptions import DatabaseError

        with pytest.raises(DatabaseError):
            conn.cursor().execute(query + " HAVING COUNT(*) > 10 ORDER BY evt.v")
    finally:
        conn.close()


@pytest.mark.parametrize("v2", [False, True])
@pytest.mark.parametrize("distinct", [False, True])
def test_aggregation_at_select_size_limit(review_index, v2, distinct):
    url, index = review_index
    conn = connection(url, v2)
    try:
        projection = "DISTINCT k" if distinct else "k, COUNT(*)"
        grouping = "" if distinct else " GROUP BY k"
        rows = (
            conn.cursor()
            .execute(f"SELECT {projection} FROM `{index}` WHERE v < 200{grouping}")
            .fetchall()
        )
        assert len(rows) == 200
        assert {row[0] for row in rows} == {f"key{i:05d}" for i in range(200)}
    finally:
        conn.close()


@pytest.mark.parametrize("v2", [False, True])
@pytest.mark.parametrize("distinct", [False, True])
def test_aggregation_bucket_ceiling_is_not_silent(review_index, v2, distinct):
    from es.exceptions import DataError

    url, index = review_index
    conn = connection(url, v2)
    projection = "DISTINCT k" if distinct else "k, COUNT(*)"
    grouping = "" if distinct else " GROUP BY k"
    query = f"SELECT {projection} FROM `{index}`{grouping}"
    try:
        for limit in ("", " LIMIT 1001") if v2 else ("",):
            with pytest.raises(DataError, match="bucket limit"):
                conn.cursor().execute(query + limit)
        if not v2:
            # An explicit LIMIT lets the legacy engine return more buckets.
            assert len(conn.cursor().execute(query + " LIMIT 1001").fetchall()) == 1001
        for limit in (5, 1000):
            assert (
                len(conn.cursor().execute(query + f" LIMIT {limit}").fetchall())
                == limit
            )
    finally:
        conn.close()


def test_elasticsearch_dst_time_zone_returns_utc():
    if os.environ.get("ES_DRIVER", "elasticsearch") != "elasticsearch":
        pytest.skip("Elasticsearch returns offsets; OpenSearch ignores time_zone")
    from es.elastic.api import connect as elastic_connect

    url = urlsplit(os.environ.get("ES_URI", "http://localhost:9200"))
    conn = elastic_connect(
        host=url.hostname, port=url.port, scheme=url.scheme, time_zone="Europe/Berlin"
    )
    client = conn.es
    index = "review-dst-" + uuid.uuid4().hex
    try:
        client.indices.create(
            index=index,
            body={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"ts": {"type": "date"}}},
            },
        )
        client.bulk(
            body=[
                {"index": {"_index": index}},
                {"ts": "2024-01-01T11:00:00Z"},
                {"index": {"_index": index}},
                {"ts": "2024-07-01T10:00:00Z"},
            ],
            refresh=True,
        )
        cursor = conn.cursor().execute(f'SELECT ts FROM "{index}" ORDER BY ts')
        assert cursor.description[0].type == Type.DATETIME
        rows = cursor.fetchall()
        assert rows == [
            (datetime.datetime(2024, 1, 1, 11, tzinfo=datetime.timezone.utc),),
            (datetime.datetime(2024, 7, 1, 10, tzinfo=datetime.timezone.utc),),
        ]
        assert all(row[0].tzinfo is datetime.timezone.utc for row in rows)
    finally:
        client.indices.delete(index=index, ignore=404)
        conn.close()
