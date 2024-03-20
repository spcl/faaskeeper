from abc import ABC, abstractmethod
from typing import Union

class Storage(ABC):
    def __init__(self, storage_name: str):
        self._storage_name = storage_name

    @property
    def storage_name(self) -> str:
        return self._storage_name

    @abstractmethod
    def write(self, key: str, data: Union[dict, bytes]):
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

    @abstractmethod
    def read(self, key: str):
        """
        Read contents stored in the object/row in the storage.
        """
        pass

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
