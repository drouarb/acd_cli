from sqlalchemy import Column, String, ForeignKey, BigInteger
from .base import Base


class Files(Base):
    __tablename__ = 'files'
    id = Column(String(50), ForeignKey('nodes.id'), primary_key=True, nullable=False, unique=True)
    md5 = Column(String(32))
    size = Column(BigInteger)
