from abc import ABC, abstractmethod


class Storage(ABC):
    def _toSchema(self, key: str, data: str):
        return {
            "path": {"S": key},
            "data": {"B": data},
            "dFxid": {"N": "0"},
            "cFxid": {"N": "0"},
            "mFxid": {"N": "0"},
            "ephemeralOwner": {"S": ""},
        }

    @abstractmethod
    def write(self, storage_name: str, key: str, data: str):
        pass

    @abstractmethod
    def update(self, storage_name: str, key: str, data: dict):
        pass

    @abstractmethod
    def read(self, storage_name: str, key: str):
        pass

    @abstractmethod
    def delete(self, storage_name: str, key: str):
        pass

    @abstractmethod
    def errorSupplier(self):
        pass
