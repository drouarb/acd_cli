import enum

from sqlalchemy import Column, String, DateTime, Enum
from .base import Base


class Status(enum.Enum):
    AVAILABLE = "AVAILABLE"
    TRASH = "TRASH"
    PURGED = "PURGED"
    PENDING = "PENDING"


class Nodes(Base):
    __tablename__ = 'nodes'
    id = Column(String(50), primary_key=True, nullable=False, unique=True)
    type = Column(String(15))
    name = Column(String(256))
    description = Column(String(500))
    created = Column(DateTime())
    modified = Column(DateTime())
    updated = Column(DateTime())
    status = Column(Enum(Status))

    def __lt__(self, other):
        return self.name < other.name

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return 'Node(%r, %r)' % (self.id, self.name)

    @property
    def is_folder(self):
        return self.type == 'folder'

    @property
    def is_file(self):
        return self.type == 'file'

    @property
    def is_available(self):
        return self.status == Status.AVAILABLE

    @property
    def is_trashed(self):
        return self.status == Status.TRASH

    @property
    def simple_name(self):
        if self.is_file:
            return self.name
        return (self.name if self.name else '') + '/'
