from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from typing import List

from es import basesqlalchemy
import es.elastic

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
    def dbapi(cls):
        return es.elastic

    def get_table_names(self, connection, schema=None, **kwargs) -> List[str]:
        query = "SHOW VALID_TABLES"
        result = connection.execute(query)
        # return a list of table names exclude hidden and empty indexes
        return [table.name for table in result if table.name[0] != "."]

    def get_view_names(self, connection, schema=None, **kwargs) -> List[str]:
        query = "SHOW VALID_VIEWS"
        result = connection.execute(query)
        # return a list of view names (ES aliases) exclude hidden and empty indexes
        return [table.name for table in result if table.name[0] != "."]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = f'SHOW COLUMNS FROM "{table_name}"'
        # A bit of an hack this cmd does not exist on ES
        array_columns_ = connection.execute(
            f"SHOW ARRAY_COLUMNS FROM {table_name}"
        ).fetchall()
        if len(array_columns_[0]) == 0:
            array_columns = []
        else:
            array_columns = [col_name[0] for col_name in array_columns_]

        result = connection.execute(query)
        return [
            {
                "name": row.column,
                "type": basesqlalchemy.get_type(row.mapping),
                "nullable": True,
                "default": None,
            }
            for row in result
            if row.mapping not in self._not_supported_column_types
            and row.column not in array_columns
        ]


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):

    scheme = "https"
    default_paramstyle = "pyformat"
