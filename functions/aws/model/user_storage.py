from abc import ABC, abstractmethod
from typing import Union

from functions.aws.control import DynamoStorage as DynamoDriver


class Storage(ABC):
    def __init__(self, storage_name: str):
        self._storage_name = storage_name

    @abstractmethod
    def write(self, key: str, data: Union[str, bytes]):
        """
            Write object or set of values to the storage.
        """
        pass

    @abstractmethod
    def update(self, key: str, data: dict):
        """
            Update existing object or set of values in the storage.
        """
        pass

    # @abstractmethod
    # def delete(self, key: str):
    #    """
    #        Remove contents stored in the object/row in the storage.
    #    """
    #    pass

    @property
    @abstractmethod
    def errorSupplier(self):
        pass


class DynamoStorage(Storage):
    def _toSchema(self, key: str, data: str):
        return {
            "path": {"S": key},
            "data": {"B": data},
            "cFxidSys": {"L": [{"N": "0"}]},
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "mFxidEpoch": {"NS": ["0"]},
            "ephemeralOwner": {"S": ""},
        }

    def __init__(self, table_name: str):
        self._storage = DynamoDriver(table_name)

    def write(self, key: str, data: str):
        return self._storage.write(key, self._toSchema(key, data))

    def update(self, key: str, data: dict):
        # FIXME define schema
        return self._storage.update(key, data)

    @property
    def errorSupplier(self):
        return self._storage.errorSupplier


class S3Storage:
    pass
