from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

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


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):

    scheme = "https"
    default_paramstyle = "pyformat"
