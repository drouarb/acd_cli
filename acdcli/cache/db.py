import logging
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from acdcli.utils.conf import get_conf

from .schema import SchemaMixin
from .query import QueryMixin
from .format import FormatterMixin
from .sync import SyncMixin
from .templates.nodes import Nodes
from .keyvaluestorage import KeyValueStorage

logger = logging.getLogger(__name__)

_DB_DEFAULT = 'nodes.db'
_SETTINGS_FILENAME = 'cache.ini'

class IntegrityError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class NodeCache(SchemaMixin, QueryMixin, FormatterMixin, SyncMixin):
    IntegrityCheckType = dict(full=0, quick=1, none=2)

    def __init__(self, cache_path: str='', settings_path=''):
        self.init_config(cache_path, settings_path)
        self._engine = create_engine(self._conf["database"]["url"])
        self.init()

        self._DBSession = sessionmaker(bind=self._engine)
        self._session = self._DBSession()

        self.KeyValueStorage = KeyValueStorage(self._session)

        self.KeyValueStorage.__setitem__("Hello", "die")

        rootNodes = self._session.query(Nodes).filter(Nodes.name == None).all()
        if len(rootNodes) > 1:
            raise IntegrityError('Could not uniquely identify root node.')
        elif len(rootNodes) == 0:
            self.root_id = ''
        else:
            self.root_id = rootNodes[0].id

    def init_config(self, cache_path, settings_path):
        _def_conf = configparser.ConfigParser()
        _def_conf['database'] = dict(url='sqlite:///' + cache_path + '/' + _DB_DEFAULT)
        _def_conf['blacklist'] = dict(folders=[])

        self._conf = get_conf(settings_path, _SETTINGS_FILENAME, _def_conf)
