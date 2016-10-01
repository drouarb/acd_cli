from .templates.metadata import Metadata


class KeyValueStorage(object):
    def __init__(self, session):
        self._session = session

    def __getitem__(self, key: str):
        return self._session.query(Metadata.value).filter(Metadata.key == key).scalar()

    def __setitem__(self, key: str, value: str):
        self._session.merge(Metadata(key=key, value=value))
        self._session.commit()

    def get(self, key: str, default: str=None):
        r = self._session.query(Metadata).filter(Metadata.key == key).first()
        return r.value if r else default

    def update(self, dict_: dict):
        for key in dict_.keys():
            self.__setitem__(key, dict_[key])
