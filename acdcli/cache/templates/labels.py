from sqlalchemy import Column, String, ForeignKey
from .base import Base


class Labels(Base):
    __tablename__ = 'labels'
    id = Column(String(50), ForeignKey('nodes.id'), primary_key=True, nullable=False)
    name = Column(String(256), primary_key=True, nullable=False)
