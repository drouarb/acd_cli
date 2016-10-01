from sqlalchemy import Column, String
from .base import Base


class Metadata(Base):
    __tablename__ = 'metadata'
    key = Column(String(64), primary_key=True, nullable=False)
    value = Column(String())
