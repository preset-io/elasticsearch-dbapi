from __future__ import annotations

import logging
from types import ModuleType
from typing import Any, List, Optional, TYPE_CHECKING

from es import basesqlalchemy
import es.opendistro
from sqlalchemy.engine import Connection
from sqlalchemy.sql import compiler, text

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn

logger = logging.getLogger(__name__)


class ESCompiler(basesqlalchemy.BaseESCompiler):  # pragma: no cover
    pass


class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):  # pragma: no cover
    pass


class ESTypeIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.initial_quote = self.final_quote = "`"


class ESDialect(basesqlalchemy.BaseESDialect):

    name = "odelasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler
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

    # SQLAlchemy 2.x (must be defined on concrete class)
    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return es.opendistro
