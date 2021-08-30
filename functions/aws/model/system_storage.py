from abc import ABC, abstractmethod

from functions.aws.control import DynamoStorage as DynamoDriver


class Storage(ABC):
    def __init__(self, storage_name: str):
        self._storage_name = storage_name

    @abstractmethod
    def delete(self, key: str):
        """
            Remove contents stored in the object/row in the storage.
        """
        pass

    @property
    @abstractmethod
    def errorSupplier(self):
        pass


class DynamoStorage:

    # FIXME: implement counter increase
    # FIXME: implement epoch counter change

    def __init__(self, table_name: str):
        self._storage = DynamoDriver(table_name)

    def delete(self, key: str):
        return self._storage.delete(key)

    def errorSupplier(self):
        return self._storage.exceptions
