from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from six.moves.urllib import parse

import requests
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql import elements
from sqlalchemy import types

import es
from . import exceptions
import logging

logger = logging.getLogger(__name__)


class ESCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        if select._offset_clause:
            raise exceptions.NotSupportedError("Offset clause is not supported in ES")
        top = None
        # Pinot does not support orderby-limit for aggregating queries, replace that with
        # top keyword. (The order by info is lost since the result is always ordered-desc by the group values)
        if select._group_by_clause is not None:
            logger.debug(
                f"Query {select} has metrics, so rewriting its order-by/limit clauses to just top"
            )
            top = 100
            if select._limit_clause is not None:
                if select._simple_int_limit:
                    top = select._limit
                else:
                    raise exceptions.NotSupportedError(
                        "Only simple integral limits are supported in ES"
                    )
                select._limit_clause = None
            select._order_by_clause = elements.ClauseList()
        return super().visit_select(select, **kwargs) + (
            f"\nTOP {top}" if top is not None else ""
        )

    def visit_column(self, column, result_map=None, **kwargs):
        # Pinot does not support table aliases
        if column.table:
            column.table.named_with_column = False
        result_map = result_map or kwargs.pop("add_to_result_map", None)
        # This is a hack to modify the original column, but how do I clone it ?
        column.is_literal = True
        return super().visit_column(column, result_map, **kwargs)

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        if kw:
            render_label_as_label = kw.pop("render_label_as_label", None)
        render_label_as_label = None
        return super().visit_label(
            label,
            add_to_result_map,
            within_label_clause,
            within_columns_clause,
            render_label_as_label,
            **kw,
        )


class ESTypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type DATETIME is not supported")

    def visit_TIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type TIME is not supported")

    def visit_BINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type BINARY is not supported")

    def visit_VARBINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type VARBINARY is not supported")

    def visit_BLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type BLOB is not supported")

    def visit_CLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type CBLOB is not supported")

    def visit_NCLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError("Type NCBLOB is not supported")


class ESDialect(default.DefaultDialect):

    name = "es"
    scheme = "http"
    driver = "rest"
    preparer = compiler.IdentifierPreparer
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_simple_order_by_label = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._server = None
        self._debug = False
        self.update_from_kwargs(kwargs)

    def update_from_kwargs(self, givenkw):
        kwargs = givenkw.copy() if givenkw else {}
        if "server" in kwargs:
            self._server = kwargs.pop("server")
        kwargs["debug"] = self._debug = bool(kwargs.get("debug", False))
        logger.info(
            f"Updated ES dialect args from {kwargs}: {self._server} and {self._debug}"
        )
        return kwargs

    @classmethod
    def dbapi(cls):
        return es

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 9000,
            "path": url.database,
            "scheme": self.scheme,
        }
        if url.query:
            kwargs.update(url.query)

        kwargs = self.update_from_kwargs(kwargs)
        return ([], kwargs)

    def get_metadata_from_controller(self, path):
        url = parse.urljoin(self._server, path)
        r = requests.get(url, headers={"Accept": "application/json"})
        try:
            result = r.json()
        except ValueError as e:
            raise exceptions.DatabaseError(
                f"Got invalid json response from {self._server}:{path}: {r.text}"
            ) from e
        if self._debug:
            logger.info(f"metadata get on {self._server}:{path} returned {result}")
        return result

    def get_schema_names(self, connection, **kwargs):
        return ["default"]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_table_names(self, connection, schema=None, **kwargs):
        return [
            spec["index"]
            for spec in self.get_metadata_from_controller("/_cat/indices?format=json")
        ]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        payload = self.get_metadata_from_controller(
            f"/{table_name}/_mapping/?format=json"
        )
        specs = payload.get(table_name, []).get("mappings").get("properties")
        columns = []
        for column_name, spec in specs.items():
            if spec.get("type") == 'alias' or not spec.get("type"):
                continue
            columns.append(
                {
                    "name": column_name,
                    "type": get_type(spec["type"], spec.get("ignore_above")),
                    "nullable": True,
                    "default": "",
                }
            )
        return columns

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
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):

    scheme = "http"
    default_paramstyle = "pyformat"


def get_default(es_column_default):
    if es_column_default == "null":
        return None
    else:
        return str(es_column_default)


def get_type(data_type, field_size):
    type_map = {
        "text": types.String,
        "keyword": types.String,
        "date": types.DATE,
        "long": types.BigInteger,
        "float": types.Float,
        "double": types.Numeric,
        "bytes": types.LargeBinary,
        "boolean": types.Boolean,
        "ip": types.String,
    }
    return type_map[data_type.lower()]
