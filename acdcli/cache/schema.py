import logging

from .templates import *

logger = logging.getLogger(__name__)


class SchemaMixin(object):
    _DB_SCHEMA_VER = 2

    def init(self):
        base.Base.metadata.create_all(self._engine, checkfirst=True)

    def drop_all(self):
        base.Base.metadata.drop_all(self._engine)
