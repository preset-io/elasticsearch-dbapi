from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from typing import List

from es import basesqlalchemy
import es.opendistro

logger = logging.getLogger(__name__)


class ESCompiler(basesqlalchemy.BaseESCompiler):  # pragma: no cover
    pass


class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):  # pragma: no cover
    pass


class ESDialect(basesqlalchemy.BaseESDialect):

    name = "odelasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler

    @classmethod
    def dbapi(cls):
        return es.opendistro

    def get_table_names(self, connection, schema=None, **kwargs) -> List[str]:
        # custom builtin query
        query = "SHOW VALID_TABLES"
        result = connection.execute(query)
        # return a list of table names exclude hidden and empty indexes
        return [table.TABLE_NAME for table in result if table.TABLE_NAME[0] != "."]

    def get_view_names(self, connection, schema=None, **kwargs):
        # custom builtin query
        query = "SHOW VALID_VIEWS"
        result = connection.execute(query)
        # return a list of table names exclude hidden and empty indexes
        return [table.VIEW_NAME for table in result if table.VIEW_NAME[0] != "."]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        # custom builtin query
        query = f"SHOW VALID_COLUMNS FROM {table_name}"

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
