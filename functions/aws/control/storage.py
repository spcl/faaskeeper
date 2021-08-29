from abc import ABC, abstractmethod


class Storage(ABC):
    def _toSchema(self, key: str, data: str):
        return {
            "path": {"S": key},
            "data": {"B": data},
            "cFxidSys": {"L": [{"N" : "0"}]},
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N" : "0"}]},
            "mFxidEpoch": {"NS": ["0"]},
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
