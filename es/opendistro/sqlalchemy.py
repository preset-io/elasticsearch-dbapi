from __future__ import annotations

import logging
from types import ModuleType
from typing import Any, List, Optional, TYPE_CHECKING

from es import basesqlalchemy
import es.opendistro
from sqlalchemy.engine import Connection
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import Label

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn

logger = logging.getLogger(__name__)


class ESCompiler(basesqlalchemy.BaseESCompiler):
    def visit_column(  # type: ignore[override]
        self, column: Any, include_table: bool = True, **kwargs: Any
    ) -> str:
        """
        Renders a column unqualified when its table is the statement's only
        FROM element.

        The OpenSearch 3 SQL engine rejects table-qualified columns in some
        statements (``SELECT flights.a FROM flights ORDER BY flights.a`` is an
        "Illegal SQL expression"), which is what SQLAlchemy emits for any Core
        ``select()`` over a ``Table``. With a single FROM element the qualifier
        is redundant. A column of any other table (a correlated reference to
        an enclosing query) keeps it.

        It is also kept when a label of the same statement has the column's
        name but another expression: in ORDER BY and GROUP BY a bare name
        resolves to the label first, so ``SELECT v AS k ... ORDER BY k``
        would sort by ``v``.
        """
        if include_table and self.stack:
            entry = self.stack[-1]
            froms = entry.get("asfrom_froms") or set()
            if (
                len(froms) == 1
                and column.table in froms
                and not self._is_shadowed_by_label(column, entry.get("selectable"))
            ):
                include_table = False
        return super().visit_column(column, include_table=include_table, **kwargs)

    @staticmethod
    def _is_shadowed_by_label(column: Any, select: Any) -> bool:
        name = str(column.name).lower()
        for element in getattr(select, "_raw_columns", None) or ():
            if not isinstance(element, Label) or element.name is None:
                continue
            if str(element.name).lower() == name and element.element is not column:
                return True
        return False


class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):  # pragma: no cover
    pass


class ESTypeIdentifierPreparer(basesqlalchemy.BaseESIdentifierPreparer):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.initial_quote = self.final_quote = "`"


class ESDialect(basesqlalchemy.BaseESDialect):

    name = "odelasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler
    supports_statement_cache = True
    preparer = ESTypeIdentifierPreparer
    _not_supported_column_types = ["nested", "geo_point", "alias"]

    # SQLAlchemy 2.x
    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return es.opendistro

    # SQLAlchemy 1.x
    @classmethod
    def dbapi(cls) -> ModuleType:  # type: ignore[override]
        return es.opendistro

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        # custom builtin query
        query = "SHOW VALID_TABLES"
        result = connection.execute(text(query))
        # return a list of table names exclude hidden and empty indexes
        return [table.TABLE_NAME for table in result if table.TABLE_NAME[0] != "."]

    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        # custom builtin query
        query = "SHOW VALID_VIEWS"
        result = connection.execute(text(query))
        # return a list of table names exclude hidden and empty indexes
        return [table.VIEW_NAME for table in result if table.VIEW_NAME[0] != "."]

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[ReflectedColumn]:
        # custom builtin query
        query = f"SHOW VALID_COLUMNS FROM {table_name}"

        result = connection.execute(text(query))
        return [
            {
                "name": row.COLUMN_NAME,
                "type": basesqlalchemy.get_type(row.TYPE_NAME),
                "nullable": True,
                "default": None,
            }
            for row in result
            if row.TYPE_NAME not in self._not_supported_column_types
        ]


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):

    scheme = "https"
    default_paramstyle = "pyformat"
    supports_statement_cache = True

    # SQLAlchemy 2.x (must be defined on concrete class)
    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return es.opendistro
