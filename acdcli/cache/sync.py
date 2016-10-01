import logging
from itertools import islice

from datetime import datetime
import dateutil.parser as iso_date

from .templates.nodes import Nodes, Status
from .templates.files import Files
from .templates.parentage import Parentage
from .templates.labels import Labels

logger = logging.getLogger(__name__)


def gen_slice(list_, length=100):
    it = iter(list_)
    while True:
        slice_ = [_ for _ in islice(it, length)]
        if not slice_:
            return
        yield slice_


class SyncMixin(object):
    def remove_purged(self, purged: list):
        if not purged:
            return

        for slice_ in gen_slice(purged):
            self._session.query(Nodes) \
                .filter(Nodes.id.in_(slice_)) \
                .delete(synchronize_session=False)

            self._session.query(Files) \
                .filter(Files.id.in_(slice_)) \
                .delete(synchronize_session=False)

            self._session.query(Parentage) \
                .filter(Parentage.parent.in_(slice_)) \
                .delete(synchronize_session=False)

            self._session.query(Parentage) \
                .filter(Parentage.child.in_(slice_)) \
                .delete(synchronize_session=False)

            self._session.query(Labels) \
                .filter(Labels.id.in_(slice_)) \
                .delete(synchronize_session=False)

        self._session.commit()
        logger.info('Purged %i node(s).' % len(purged))

    def insert_nodes(self, nodes: list, partial=True):
        files = []
        folders = []

        for node in nodes:
            if node['status'] == 'PENDING':
                continue
            kind = node['kind']
            if kind == 'FILE':
                if not 'name' in node or not node['name']:
                    logger.warning('Skipping file %s because its name is empty.' % node['id'])
                    continue
                files.append(node)
            elif kind == 'FOLDER':
                if (not 'name' in node or not node['name']) \
                        and (not 'isRoot' in node or not node['isRoot']):
                    logger.warning('Skipping non-root folder %s because its name is empty.' % node['id'])
                    continue
            elif kind != 'ASSET':
                logger.warning('Cannot insert unknown node type "%s".' % kind)
        self.insert_folders(folders)
        self.insert_files(files)

        self.insert_parentage(files + folders, partial)

    def insert_node(self, node: dict):
        if not node:
            return
        self.insert_nodes([node])

    def insert_folders(self, folders: list):
        if not folders:
            return

        for f in folders:
            self._session.merge(
                Nodes(
                    id=f['id'],
                    type="folder",
                    name=f.get('name'),
                    description=f.get('description'),
                    created=iso_date.parse(f['createdDate']),
                    modified=iso_date.parse(f['modifiedDate']),
                    updated=datetime.utcnow(),
                    status=Status(f['status'])
                ))

        self._session.commit()
        logger.info('Inserted/updated %d folder(s).' % len(folders))

    def insert_files(self, files: list):
        if not files:
            return

        for f in files:
            self._session.merge(
                Nodes(
                    id=f['id'],
                    type="file",
                    name=f.get('name'),
                    description=f.get('description'),
                    created=iso_date.parse(f['createdDate']),
                    modified=iso_date.parse(f['modifiedDate']),
                    updated=datetime.utcnow(),
                    status=(Status(f['status']))
                ))
            self._session.merge(
                Files(
                    id=f['id'],
                    md5=f.get('contentProperties', {}).get('md5', 'd41d8cd98f00b204e9800998ecf8427e'),
                    size=f.get('contentProperties', {}).get('size', 0)
                ))

        self._session.commit()
        logger.info('Inserted/updated %d file(s).' % len(files))

    def insert_parentage(self, nodes: list, partial=True):
        if not nodes:
            return

        if partial:
            for slice_ in gen_slice(nodes):
                self._session.query(Parentage) \
                    .filter(Parentage.child.in_([n['id'] for n in slice_])) \
                    .delete(synchronize_session=False)

            self._session.commit()

        for n in nodes:
            for p in n['parents']:
                self._session.merge(Parentage(
                    parent=p,
                    child=n['id']
                ))

        self._session.commit()
        logger.info('Parented %d node(s).' % len(nodes))
