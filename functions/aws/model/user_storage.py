import struct
from abc import ABC, abstractmethod
from enum import Enum
from functools import reduce
from typing import Set

from functions.aws.control import DynamoStorage as DynamoDriver
from functions.aws.control import S3Storage as S3Driver


class OpResult(Enum):
    SUCCESS = 0
    NODE_EXISTS = 1


class Storage(ABC):
    def __init__(self, storage_name: str):
        self._storage_name = storage_name

    @abstractmethod
    def write(
        self,
        key: str,
        data: bytes,
        created_sys_counter: dict,
        modified_sys_counter: dict,
    ):
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
    def _toSchema(self, key: str, data: bytes, created: dict, modified: dict):
        # FIXME: pass counter value
        return {
            "path": {"S": key},
            "data": {"B": data},
            "cFxidSys": created,
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": modified,
            "mFxidEpoch": {"NS": ["0"]},
            "ephemeralOwner": {"S": ""},
        }

    def __init__(self, table_name: str):
        self._storage = DynamoDriver(table_name, "path")

    def write(
        self,
        key: str,
        data: bytes,
        created_sys_counter: dict,
        modified_sys_counter: dict,
    ):
        try:
            self._storage.write(
                key,
                self._toSchema(key, data, created_sys_counter, modified_sys_counter),
            )
            return OpResult.SUCCESS
        except self.errorSupplier.ConditionalCheckFailedException:
            return OpResult.NODE_EXISTS

    def update(self, key: str, data: dict):
        # FIXME define schema
        return self._storage.update(key, data)

    @property
    def errorSupplier(self):
        return self._storage.errorSupplier


class S3Storage:
    def _serialize(self, created: dict, modified: dict) -> bytes:
        # FIXME: pass counter value
        created_system = [int(value) for v in created["L"] for key, value in v.items()]
        created_epoch: Set[int] = set()
        modified_system = [
            int(value) for v in modified["L"] for key, value in v.items()
        ]
        modified_epoch: Set[int] = set()

        counters = [created_system, created_epoch, modified_system, modified_epoch]
        total_length = reduce(lambda a, b: a + b, map(len, counters))
        return struct.pack(
            f"{5+total_length}I",
            4 + total_length,
            len(created_system),
            *created_system,
            len(created_epoch),
            *created_epoch,
            len(modified_system),
            *modified_system,
            len(modified_epoch),
            *modified_epoch,
        )

    def __init__(self, bucket_name: str):
        self._storage = S3Driver(bucket_name)

    def write(
        self,
        key: str,
        data: bytes,
        created_sys_counter: dict,
        modified_sys_counter: dict,
    ):
        self._storage.write(
            key, self._serialize(created_sys_counter, modified_sys_counter) + data
        )
        return OpResult.SUCCESS

    @property
    def errorSupplier(self):
        return self._storage.errorSupplier
