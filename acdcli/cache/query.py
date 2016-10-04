import re

from sqlalchemy import func

from .templates.nodes import Nodes, Status
from .templates.files import Files
from .templates.parentage import Parentage


class QueryMixin(object):
    def get_node(self, id) -> 'Union[Nodes|None]':
        return self._session.query(Nodes).filter(Nodes.id == id)

    def get_root_node(self):
        return self.get_node(self.root_id)

    def get_conflicting_node(self, name: str, parent_id: str):
        folders, files = self.list_children(parent_id)
        for n in folders + files:
            if n.is_available and n.name.lower() == name.lower():
                return n

    def resolve(self, path: str, trash=False) -> 'Union[Nodes|None]':
        segments = list(filter(bool, path.split('/')))
        if not segments:
            if not self.root_id:
                return
            return self._session.query(Nodes).filter(Nodes.id == self.root_id).first()

        parent = self.root_id
        for i, segment in enumerate(segments):
            result = self._session.query(Nodes) \
                .join(Parentage, Parentage.child == Nodes.id) \
                .filter(Parentage.parent == parent) \
                .filter(Nodes.name == segment).first()

            if not result:
                return
            if not result.is_available:
                if not trash:
                    return
            if i + 1 == segments.__len__():
                if result.is_file:
                    result.size = self._session.query(Files.size) \
                        .filter(Files.id == result.id) \
                        .scalar()
                return result
            if result.is_folder:
                parent = result.id
            else:
                return

    def childrens_names(self, folder_id) -> 'List[str]':
        names = self._session.query(Nodes.name) \
            .join(Parentage, Parentage.child == Nodes.id) \
            .filter(Parentage.parent == folder_id) \
            .filter(Nodes.status == Status.AVAILABLE) \
            .all()
        return [name[0] for name in names]

    def get_node_count(self) -> int:
        return self._session.query(func.count(Nodes.id)).scalar()

    def get_files_count(self) -> int:
        return self._session.query(func.count(Files.id)).scalar()

    def get_folders_count(self) -> int:
        return self._session.query(func.count(Nodes.id)).filter(Nodes.type == "folder").scalar()

    def calculate_usage(self):
        return self._session.query(func.sum(Files.size)).scalar()

    def num_children(self, folder_id) -> int:
        return self._session.query(func.count(Nodes.name)) \
            .join(Parentage, Parentage.child == Nodes.id) \
            .filter(Parentage.parent == folder_id) \
            .filter(Nodes.status == Status.AVAILABLE) \
            .scalar()

    def num_parents(self, node_id) -> int:
        return self._session.query(func.count(Nodes.name)) \
            .join(Parentage, Parentage.parent == Nodes.id) \
            .filter(Parentage.child == node_id) \
            .filter(Nodes.status == Status.AVAILABLE) \
            .scalar()

    def get_child(self, folder_id, child_name) -> 'Union[Nodes|None]':
        return self._session.query(Nodes) \
            .join(Parentage, Parentage.child == Nodes.id) \
            .filter(Parentage.parent == folder_id) \
            .filter(Nodes.name == child_name) \
            .filter(Nodes.status == Status.AVAILABLE) \
            .first()

    def list_children(self, folder_id, trash=False) -> 'Tuple[List[Nodes], List[Nodes]]':
        files = []
        folders = []

        results = self._session.query(Nodes) \
            .join(Parentage, Parentage.child == Nodes.id) \
            .filter(Parentage.parent == folder_id).all()

        for result in results:
            if result.is_available or trash:
                if result.is_file:
                    files.append(result)
                elif result.is_folder:
                    folders.append(result)

        return folders, files

    def list_trashed_children(self, folder_id) -> 'Tuple[List[Nodes], List[Nodes]]':
        folders, files = self.list_children(folder_id, True)
        folders[:] = [f for f in folders if f.is_trashed]
        files[:] = [f for f in files if f.is_trashed]
        return folders, files

    def first_path(self, node_id: str) -> str:
        if node_id == self.root_id:
            return '/'
        node = self._session.query(Nodes) \
            .join(Parentage, Parentage.parent == Nodes.id) \
            .filter(Parentage.child == node_id) \
            .order_by(Nodes.status) \
            .order_by(Nodes.id) \
            .first()
        if node.id == self.root_id:
            return node.simple_name
        return self.first_path(node.id) + node.name + '/'

    def find_by_name(self, name: str) -> 'List[Nodes]':
        return self._session.query(Nodes) \
            .filter(Nodes.name.like("%" + name + "%")) \
            .order_by(Nodes.name) \
            .all()

    def find_by_md5(self, md5) -> 'List[Nodes]':
        return self._session.query(Nodes) \
            .join(Files, Files.id == Nodes.id) \
            .filter(Files.md5 == md5) \
            .order_by(Nodes.name) \
            .all()

    def find_by_regex(self, regex) -> 'List[Nodes]':
        if self._conf["database"]["url"].startswith("sqlite") or self._conf["database"]["url"].startswith("mysql"):
            return self._session.query(Nodes) \
                .filter(Nodes.name.op("REGEXP")(regex)) \
                .order_by(Nodes.name) \
                .all()

        nodes = self._session.query(Nodes).order_by(Nodes.name).all()
        match = []
        for n in nodes:
            if re.match(regex, n.name):
                match.append(n)
        return match

    def file_size_exists(self, size) -> bool:
        return bool(self._session.query(func.count(Files.id)) \
                    .join(Nodes, Nodes.id == Files.id) \
                    .filter(Files.size == size) \
                    .filter(Nodes.status == Status.AVAILABLE).scalar())
