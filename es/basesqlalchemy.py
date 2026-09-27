from __future__ import annotations

import logging
import re
from typing import Any, List, Optional, Tuple, Type, TYPE_CHECKING

import es
from es import exceptions
from es.baseapi import parse_bool_argument
from es.const import DEFAULT_SCHEMA
from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn


logger = logging.getLogger(__name__)


class BaseESIdentifierPreparer(compiler.IdentifierPreparer):
    """
    Keeps the dummy ``default`` schema out of compiled SQL.

    Elasticsearch has no schemas; the dialect reports ``default`` so that
    tools which require one (Superset datasets, reflection) have a name to
    use. SQLAlchemy qualifies every table *and every table-bound column*
    with that schema, and neither SQL endpoint knows it: Elasticsearch
    rejects ``"default".flights."Carrier"`` and OpenSearch resolves it to
    NULL. Rendering the dummy schema like ``schema=None`` fixes both the FROM
    clause and the projection at compile time, instead of rewriting the
    statement text afterwards.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.schema_for_object = self._omit_dummy_schema  # type: ignore[assignment]

    @staticmethod
    def _omit_dummy_schema(obj: Any) -> Optional[str]:
        schema = obj.schema
        return None if schema == DEFAULT_SCHEMA else schema

    def _render_schema_translates(
        self, statement: str, schema_translate_map: Any
    ) -> str:
        # With a schema_translate_map the schema is only known at execution
        # time: SQLAlchemy renders a placeholder followed by a dot. Drop both
        # when the placeholder resolves to the dummy schema.
        translate = dict(schema_translate_map)
        if None in translate:
            translate["_none"] = translate[None]

        def omit(match: "re.Match[str]") -> str:
            name = match.group(1)
            effective = translate.get(name, None if name == "_none" else name)
            return "" if effective == DEFAULT_SCHEMA else match.group(0)

        statement = re.sub(r"__\[SCHEMA_([^\]]+)\]\.", omit, statement)
        return super()._render_schema_translates(statement, schema_translate_map)


class BaseESCompiler(compiler.SQLCompiler):
    def visit_fromclause(self, fromclause: str, **kwargs: Any):
        return fromclause.replace("default.", "")

    def visit_label(self, *args, **kwargs):
        if len(kwargs) == 0 or len(kwargs) == 1:
            kwargs["render_label_as_label"] = args[0]
        result = super().visit_label(*args, **kwargs)
        return result


class BaseESTypeCompiler(compiler.GenericTypeCompiler):
    def visit_es_field_type(self, type_: "ESFieldType", **kwargs: Any) -> str:
        # Reflected columns render the field's own mapping type
        return type_.field_type_name

    def visit_REAL(self, type_, **kwargs: Any) -> str:
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs: Any) -> str:
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs: Any) -> str:
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs: Any) -> str:
        return "DATETIME"

    def visit_TIME(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type TIME is not supported")

    def visit_BINARY(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type BINARY is not supported")

    def visit_VARBINARY(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type VARBINARY is not supported")

    def visit_BLOB(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type BLOB is not supported")

    def visit_CLOB(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type CBLOB is not supported")

    def visit_NCLOB(self, type_, **kwargs: Any) -> str:
        raise exceptions.NotSupportedError("Type NCBLOB is not supported")


class BaseESDialect(default.DefaultDialect):

    name = "SET"
    scheme = "SET"
    driver = "SET"
    statement_compiler: Type[BaseESCompiler] = BaseESCompiler
    type_compiler: Type[BaseESTypeCompiler] = BaseESTypeCompiler
    preparer = BaseESIdentifierPreparer
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = True
    # The compilers keep no per-statement state outside SQLAlchemy's own, so
    # compiled statements can be cached. SQLAlchemy only honours this flag
    # when set on each concrete dialect class, so subclasses repeat it.
    supports_statement_cache = True

    _not_supported_column_types = ["object", "nested"]

    _map_parse_connection_parameters = {
        "verify_certs": parse_bool_argument,
        "use_ssl": parse_bool_argument,
        "http_compress": parse_bool_argument,
        "sniff_on_start": parse_bool_argument,
        "sniff_on_connection_fail": parse_bool_argument,
        "retry_on_timeout": parse_bool_argument,
        "sniffer_timeout": int,
        "sniff_timeout": int,
        "max_retries": int,
        "maxsize": int,
        "timeout": int,
    }

    # SQLAlchemy 2.x
    @classmethod
    def import_dbapi(cls):
        return es

    # SQLAlchemy 1.x
    @classmethod
    def dbapi(cls):
        return es

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 9200,
            "path": url.database,
            "scheme": self.scheme,
            "user": url.username or None,
            "password": url.password or None,
        }
        if url.query:
            kwargs.update(url.query)

        for name, parse_func in self._map_parse_connection_parameters.items():
            if name in kwargs:
                kwargs[name] = parse_func(url.query[name])

        return ([], kwargs)

    def _get_server_version_info(self, connection) -> Optional[Tuple[int, ...]]:
        """
        Reports the cluster version (``GET /``) as ``server_version_info``,
        e.g. ``(7, 17, 29)`` for Elasticsearch or ``(2, 19, 1)`` for OpenSearch.

        Best-effort: a user allowed to run SQL is not necessarily allowed to
        read cluster info, and that must not stop the connection from being
        used, so any failure leaves the version unknown.
        """
        try:
            dbapi_connection = connection.connection.dbapi_connection
            number = dbapi_connection.es.info()["version"]["number"]
            return tuple(int(part) for part in re.findall(r"\d+", number)[:3])
        except Exception as ex:  # noqa: B902
            logger.warning("Could not read the cluster version: %s", ex)
            return None

    def get_schema_names(self, connection, **kwargs):
        # ES does not have the concept of a schema
        return [DEFAULT_SCHEMA]

    def has_table(self, connection, table_name, schema=None, **kw):
        # SQLAlchemy 2.0 defines has_table as true for views as well
        return table_name in self.get_table_names(
            connection, schema
        ) or table_name in self.get_view_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs) -> List[str]:
        raise NotImplementedError()  # pragma: no cover

    def get_columns(
        self, connection, table_name, schema=None, **kw
    ) -> List[ReflectedColumn]:
        raise NotImplementedError()  # pragma: no cover

    def get_view_names(self, connection, schema=None, **kwargs):
        return []  # pragma: no cover

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass  # pragma: no cover

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


class ESFieldType:
    """
    Mixin for reflected column types that keeps the field's mapping type.

    The generic SQLAlchemy types render through the type compiler as the
    names ``CAST`` accepts (``LONG`` for every integer, numeric and boolean
    type). A reflected column should instead report the type of the field it
    comes from, e.g. ``DOUBLE`` for a ``double`` field and ``BOOLEAN`` for a
    ``boolean`` one. Each subclass keeps the behaviour of its generic base
    type and only changes how it renders. SQLAlchemy only dispatches on a
    ``__visit_name__`` set on the class itself, so every subclass sets it.
    """

    field_type_name: str


class DOUBLE(ESFieldType, types.Float):  # type: ignore[type-arg]
    __visit_name__ = "es_field_type"
    field_type_name = "DOUBLE"


class FLOAT(ESFieldType, types.Float):  # type: ignore[type-arg]
    __visit_name__ = "es_field_type"
    field_type_name = "FLOAT"


class HALF_FLOAT(ESFieldType, types.Float):  # type: ignore[type-arg]
    __visit_name__ = "es_field_type"
    field_type_name = "HALF_FLOAT"


class SCALED_FLOAT(ESFieldType, types.Float):  # type: ignore[type-arg]
    __visit_name__ = "es_field_type"
    field_type_name = "SCALED_FLOAT"


class BYTE(ESFieldType, types.SmallInteger):
    __visit_name__ = "es_field_type"
    field_type_name = "BYTE"


class SHORT(ESFieldType, types.SmallInteger):
    __visit_name__ = "es_field_type"
    field_type_name = "SHORT"


class INTEGER(ESFieldType, types.Integer):
    __visit_name__ = "es_field_type"
    field_type_name = "INTEGER"


class LONG(ESFieldType, types.BigInteger):
    __visit_name__ = "es_field_type"
    field_type_name = "LONG"


class UNSIGNED_LONG(ESFieldType, types.BigInteger):
    __visit_name__ = "es_field_type"
    field_type_name = "UNSIGNED_LONG"


class BOOLEAN(ESFieldType, types.Boolean):
    __visit_name__ = "es_field_type"
    field_type_name = "BOOLEAN"


def get_type(data_type: str) -> "types.TypeEngine[Any]":
    type_map: dict[str, "types.TypeEngine[Any]"] = {
        "boolean": BOOLEAN(),
        "date": types.DateTime(),
        "date_nanos": types.DateTime(),
        "datetime": types.DateTime(),
        # Floating point columns must stay floats: ``Numeric`` would convert
        # every value to a ``Decimal`` rounded to 10 decimal places
        "double": DOUBLE(),
        "float": FLOAT(),
        "half_float": HALF_FLOAT(),
        "scaled_float": SCALED_FLOAT(),
        "text": types.String(),
        "keyword": types.String(),
        "constant_keyword": types.String(),
        "wildcard": types.String(),
        "match_only_text": types.String(),
        "version": types.String(),
        # ES returns binary fields as base64 strings
        "binary": types.String(),
        "byte": BYTE(),
        "short": SHORT(),
        "integer": INTEGER(),
        "long": LONG(),
        "unsigned_long": UNSIGNED_LONG(),
        "geo_point": types.String(),
        # TODO get a solution for nested type
        "nested": types.String(),
        # TODO get a solution for object
        "object": types.BLOB(),
        "ip": types.String(),
    }
    type_ = type_map.get(data_type)
    if not type_:
        logger.warning(f"Unknown type found {data_type} reverting to string")
        type_ = types.String()
    return type_
