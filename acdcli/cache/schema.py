import os
import logging

from .templates import *

logger = logging.getLogger(__name__)


class SchemaMixin(object):
    _DB_SCHEMA_VER = 2

    def init(self):
        base.Base.metadata.create_all(self._engine, checkfirst=True)

    def drop_all(self):
        self._session.commit()
        base.Base.metadata.drop_all(self._engine)
        return True

    def remove_db_file(self):
        if self._conf["database"]["url"].startswith("sqlite://"):
            try:
                os.remove(self._conf["database"]["url"][9:])
                logger.info('Database removed.')
            except OSError:
                logger.info('Database file was not deleted.')
                return False
        else:
            logger.info('''Database is not sqlite, can't remove db file, droping all tables.''')
            self.drop_all()
        return True
