from sqlalchemy import Column, String, ForeignKey
from .base import Base


class Parentage(Base):
    __tablename__ = 'parentage'
    parent = Column(String(50), primary_key=True, nullable=False)
    child = Column(String(50), ForeignKey('nodes.id'), primary_key=True, nullable=False)
