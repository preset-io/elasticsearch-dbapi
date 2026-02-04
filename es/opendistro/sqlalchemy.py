import logging
from types import ModuleType
from typing import Any, List, Optional

from es import basesqlalchemy
import es.opendistro
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.interfaces import DBAPIModule, ReflectedColumn
from sqlalchemy.sql import compiler

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
    supports_statement_cache = False

    @classmethod
    def import_dbapi(cls) -> ModuleType:
        return es.opendistro

    # Keep dbapi() for SQLAlchemy 1.4 backward compatibility
    @classmethod
    def dbapi(cls) -> Optional[DBAPIModule]:  # type: ignore[override]
        return cls.import_dbapi()

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        # custom builtin query
        query = text("SHOW VALID_TABLES")
        result = connection.execute(query)
        # return a list of table names exclude hidden and empty indexes
        return [table.TABLE_NAME for table in result if table.TABLE_NAME[0] != "."]

    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        # custom builtin query
        query = text("SHOW VALID_VIEWS")
        result = connection.execute(query)
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
        query = text(f"SHOW VALID_COLUMNS FROM {table_name}")

        result = connection.execute(query)
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
    _not_supported_column_types = ["nested", "geo_point", "alias"]
