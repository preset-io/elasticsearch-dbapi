from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from es import basesqlalchemy
import es.opendistro

logger = logging.getLogger(__name__)


class ESCompiler(basesqlalchemy.BaseESCompiler):  # pragma: no cover
    pass


class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):  # pragma: no cover
    pass


class ESDialect(basesqlalchemy.BaseESDialect):  # pragma: no cover

    name = "odelasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler

    @classmethod
    def dbapi(cls):
        return es.opendistro


ESHTTPDialect = ESDialect


class ESHTTPSDialect(ESDialect):  # pragma: no cover

    scheme = "https"
    default_paramstyle = "pyformat"
