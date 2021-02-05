import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from es import basesqlalchemy
import es.elastic
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


class ESCompiler(basesqlalchemy.BaseESCompiler):
    pass


class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):
    pass


class ESDialect(basesqlalchemy.BaseESDialect):

    name = "elasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler

    @classmethod
    def dbapi(cls) -> ModuleType:
        return es.elastic

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        query = "SHOW VALID_TABLES"
        result = connection.execute(query)
        # return a list of table names exclude hidden and empty indexes
        return [table.name for table in result if table.name[0] != "."]

    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        query = "SHOW VALID_VIEWS"
        result = connection.execute(query)
        # return a list of view names (ES aliases) exclude hidden and empty indexes
        return [table.name for table in result if table.name[0] != "."]

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        query = f'SHOW COLUMNS FROM "{table_name}"'
        # Custom SQL
        array_columns_ = connection.execute(
            f"SHOW ARRAY_COLUMNS FROM {table_name}"
        ).fetchall()
        # convert cursor rows: List[Tuple[str]] to List[str]
        if not array_columns_:
            array_columns = []
        else:
            array_columns = [col_name.name for col_name in array_columns_]

        all_columns = connection.execute(query)
        return [
            {
                "name": row.column,
                "type": basesqlalchemy.get_type(row.mapping),
                "nullable": True,
                "default": None,
            }
            for row in all_columns
            if row.mapping not in self._not_supported_column_types
            and row.column not in array_columns
        ]


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):

    scheme = "https"
    default_paramstyle = "pyformat"
