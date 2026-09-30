"""Keep the portable reflected types castable on real SQL backends."""

import os
import uuid

from es.tests.fixtures.fixtures import _get_client
import pytest
import sqlalchemy as sa


@pytest.mark.skipif(not os.environ.get("ES_URI"), reason="requires a live cluster")
def test_reflected_types_cast_and_copy_to_sqlite():
    uri = os.environ["ES_URI"]
    driver = os.environ.get("ES_DRIVER", "elasticsearch")
    client = _get_client(uri)
    index = "release-types-" + uuid.uuid4().hex
    fields = [
        "double",
        "float",
        "half_float",
        "scaled_float",
        "byte",
        "short",
        "integer",
        "long",
        "boolean",
    ]
    # The OpenSearch SQL plugin does not expose unsigned_long fields.
    if driver == "elasticsearch":
        fields.append("unsigned_long")
    properties = {name: {"type": name} for name in fields}
    properties["scaled_float"]["scaling_factor"] = 100
    values = {name: True if name == "boolean" else 7 for name in fields}
    client.indices.create(index=index, body={"mappings": {"properties": properties}})
    try:
        client.index(index=index, id="1", body=values, refresh=True)
        modes = [False, True] if driver == "odelasticsearch" else [False]
        for v2 in modes:
            engine = sa.create_engine(
                uri.replace("http", driver + "+http", 1) + "/?v2=" + str(v2).lower()
            )
            try:
                reflected = sa.Table(index, sa.MetaData(), autoload_with=engine)
                with engine.connect() as connection:
                    for name in fields:
                        column = reflected.c[name]
                        rows = connection.execute(
                            sa.select(sa.cast(column, column.type))
                        ).all()
                        assert len(rows) == 1
                        assert rows[0][0] == (1 if name == "boolean" else 7)
                copied_metadata = sa.MetaData()
                reflected.to_metadata(copied_metadata)
                target = sa.create_engine("sqlite://")
                try:
                    copied_metadata.create_all(target)
                finally:
                    target.dispose()
            finally:
                engine.dispose()
    finally:
        client.indices.delete(index=index)
        client.close()
