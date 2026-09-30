"""Reflected numeric/boolean types retain generic SQLAlchemy portability."""

from es.basesqlalchemy import get_type
import pytest
import sqlalchemy as sa
from sqlalchemy import types
from sqlalchemy.dialects import mysql, postgresql, sqlite


FIELD_TYPES = [
    ("double", types.Float),
    ("float", types.Float),
    ("half_float", types.Float),
    ("scaled_float", types.Float),
    ("byte", types.SmallInteger),
    ("short", types.SmallInteger),
    ("integer", types.Integer),
    ("long", types.BigInteger),
    ("unsigned_long", types.BigInteger),
    ("boolean", types.Boolean),
]


@pytest.mark.parametrize("field_type,generic", FIELD_TYPES)
@pytest.mark.parametrize(
    "dialect", [sqlite.dialect(), postgresql.dialect(), mysql.dialect()]
)
def test_reflected_type_compiles_like_generic_type(field_type, generic, dialect):
    reflected = get_type(field_type)
    assert reflected.compile(dialect=dialect) == generic().compile(dialect=dialect)
    assert reflected.copy().compile(dialect=dialect) == generic().compile(
        dialect=dialect
    )


def test_copy_reflected_table_to_sqlite():
    source = sa.Table(
        "copied_fields",
        sa.MetaData(),
        *(sa.Column(name, get_type(name)) for name, _ in FIELD_TYPES),
    )
    target_metadata = sa.MetaData()
    target = source.to_metadata(target_metadata)
    engine = sa.create_engine("sqlite://")
    target_metadata.create_all(engine)
    values = {
        name: (
            True
            if name == "boolean"
            else 1.5 if "float" in name or name == "double" else 7
        )
        for name, _ in FIELD_TYPES
    }
    with engine.begin() as connection:
        connection.execute(target.insert().values(**values))
        row = connection.execute(sa.select(target)).one()
        assert dict(row._mapping) == values
