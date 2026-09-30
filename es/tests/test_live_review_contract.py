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
                        "vf": {"type": "double"},
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
                        "vf": float(i),
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
        query = f"SELECT {projection} FROM `{index}` WHERE v < 200{grouping}"
        if old_bucket_ceiling(conn):
            from es.exceptions import DataError

            with pytest.raises(DataError, match="bucket limit"):
                conn.cursor().execute(query)
            # Only a LIMIT <= the old bucket ceiling explains the count.
            query += " LIMIT 200"
        rows = conn.cursor().execute(query).fetchall()
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
        if v2 and old_bucket_ceiling(conn):
            with pytest.raises(DataError, match="bucket limit"):
                conn.cursor().execute(query + " LIMIT 1000")
        for limit in (5, 200 if v2 and old_bucket_ceiling(conn) else 1000):
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


def old_bucket_ceiling(conn):
    version = conn.es.info()["version"]
    number = tuple(int(n) for n in version["number"].split(".")[:2])
    return version.get("distribution") == "opensearch" and (2, 0) <= number < (2, 18)


@pytest.mark.parametrize("limit", [201, 1001, 10000])
def test_v2_limited_select_with_exactly_200_matches(review_index, limit):
    url, index = review_index
    conn = connection(url, True)
    try:
        rows = (
            conn.cursor()
            .execute(f"SELECT k FROM `{index}` WHERE v < 200 LIMIT {limit}")
            .fetchall()
        )
        assert len(rows) == 200
        assert {row[0] for row in rows} == {f"key{i:05d}" for i in range(200)}
    finally:
        conn.close()


@pytest.mark.parametrize("distinct", [False, True])
def test_old_v2_450_buckets_are_not_silently_cut(review_index, distinct):
    from es.exceptions import DataError

    url, index = review_index
    conn = connection(url, True)
    projection = "DISTINCT k" if distinct else "k, COUNT(*)"
    grouping = "" if distinct else " GROUP BY k"
    try:
        for limit in ("", " LIMIT 1000"):
            query = f"SELECT {projection} FROM `{index}` WHERE v < 450{grouping}{limit}"
            if old_bucket_ceiling(conn):
                with pytest.raises(DataError, match="bucket limit"):
                    conn.cursor().execute(query)
            else:
                assert len(conn.cursor().execute(query).fetchall()) == 450
    finally:
        conn.close()


@pytest.mark.parametrize("v2", [False, True])
@pytest.mark.parametrize("limit", [11000, 20000])
def test_select_limit_above_window_is_complete_or_refused(review_index, v2, limit):
    from es.exceptions import DataError

    url, index = review_index
    conn = connection(url, v2)
    try:
        query = f"SELECT k FROM `{index}` LIMIT {limit}"
        if conn.es.info()["version"].get("distribution") != "opensearch":
            # Open Distro's SQL cursors are disabled by default.
            with pytest.raises(DataError, match="cursors"):
                conn.cursor().execute(query)
        else:
            rows = conn.cursor().execute(query).fetchall()
            assert len(rows) == min(limit, 12000)
            assert len({row[0] for row in rows}) == len(rows)
    finally:
        conn.close()


@pytest.fixture
def top_n_index(review_index):
    url, _ = review_index
    client = OpenSearch(url)
    index = "review-top-n-" + uuid.uuid4().hex
    try:
        client.indices.create(
            index=index,
            body={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"k": {"type": "keyword"}}},
            },
        )
        keys = [f"k{i:05d}" for i in range(3000)]
        for i in range(1, 6):
            keys.extend([f"z{i}"] * (i + 5))
        body = []
        for key in keys:
            body.extend([{"index": {"_index": index}}, {"k": key}])
        assert not client.bulk(body=body, refresh=True)["errors"]
        yield url, index
    finally:
        client.indices.delete(index=index, ignore=404)
        client.close()


@pytest.mark.parametrize("v2", [False, True])
def test_grouped_top_n_is_correct_or_refused(top_n_index, v2):
    from es.exceptions import DataError

    url, index = top_n_index
    conn = connection(url, v2)
    query = f"SELECT k, COUNT(*) AS c FROM `{index}` GROUP BY k ORDER BY c DESC LIMIT 5"
    try:
        expected = [(f"z{i}", i + 5) for i in range(5, 0, -1)]
        if v2:
            with pytest.raises(DataError, match="bucket limit"):
                conn.cursor().execute(query)
            # A small underlying group listing is safe, even with HAVING.
            query = (
                f"SELECT k, COUNT(*) AS c FROM `{index}` WHERE k LIKE 'z%' "
                "GROUP BY k HAVING c > 5 ORDER BY c DESC LIMIT 5"
            )
        assert conn.cursor().execute(query).fetchall() == expected
    finally:
        conn.close()


def test_custom_small_aggregation_size_limit_is_not_silent(review_index):
    from es.exceptions import DataError

    url, index = review_index
    conn = connection(url, True)
    version = conn.es.info()["version"]
    if version.get("distribution") != "opensearch" or version["number"].startswith(
        "1."
    ):
        conn.close()
        pytest.skip("only OpenSearch 2.x/3.x caps buckets at query.size_limit")
    settings = conn.es.cluster.get_settings(flat_settings=True)
    previous = settings.get("transient", {}).get("plugins.query.size_limit")
    try:
        conn.es.cluster.put_settings(
            body={"transient": {"plugins.query.size_limit": 200}}
        )
        for query in (
            f"SELECT k, COUNT(*) FROM `{index}` WHERE v < 450 GROUP BY k",
            f"SELECT DISTINCT k FROM `{index}` WHERE v < 450 LIMIT 1000",
            f"SELECT k, COUNT(*) AS c FROM `{index}` GROUP BY k ORDER BY c DESC LIMIT 5",
        ):
            with pytest.raises(DataError, match="bucket limit"):
                conn.cursor().execute(query)
    finally:
        conn.es.cluster.put_settings(
            body={"transient": {"plugins.query.size_limit": previous}}
        )
        conn.close()


def test_v2_plain_and_limited_select_have_identical_types(review_index):
    url, index = review_index
    conn = connection(url, True)
    try:
        for limit in ("", " LIMIT 5"):
            cursor = conn.cursor().execute(
                f"SELECT vf, ts FROM `{index}` WHERE v < 5{limit}"
            )
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert all(type(row[0]) is float for row in rows)
            assert all(type(row[1]) is datetime.datetime for row in rows)
            assert cursor.description[1].type == Type.DATETIME
    finally:
        conn.close()
