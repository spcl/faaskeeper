from abc import ABC, abstractmethod
from enum import Enum
from typing import Set

from faaskeeper.node import Node, NodeDataType
from functions.gcp.control.cloud_storage import CloudStorage as CloudStorageDriver
from faaskeeper.providers.serialization import S3Reader

class OpResult(Enum):
    SUCCESS = 0
    NODE_EXISTS = 1
    NODE_DOESNT_EXIST = 2

class Storage():
    def __init__(self) -> None:
        pass

    @abstractmethod
    def write(self, node: Node):
        """
        Write object or set of values to the storage.
        """
        pass

    @abstractmethod
    def update(self, node: Node, updates: Set[NodeDataType] = set()):
        pass

    @abstractmethod
    def delete(self, node: Node):
        pass

    @property
    @abstractmethod
    def errorSupplier(self):
        pass

class CloudStorageStorage(Storage):
    def __init__(self, bucket_name: str) -> None:
        self._storage = CloudStorageDriver(bucket_name)

    def write(self, node: Node):
        self._storage.write(node.path, S3Reader.serialize(node))
        return OpResult.SUCCESS
    
    def update(self, node: Node, updates: Set[NodeDataType] = set()):
        # read then write
        # Even though the S3 serializer on the client side is used, it can be used as a general one.
        if not node.has_data or not node.has_children or not node.has_created:
            node_data = self._storage.read(node.path)
            # if there is no children property, then deserialize and fetch the property.
            read_node = S3Reader.deserialize(
                node.path, node_data, not node.has_data, not node.has_children
            )
            # has_children False, 
            if not node.has_data:
                node.data = read_node.data
            if not node.has_children:
                node.children = read_node.children
            if not node.has_created:
                node.created = read_node.created
            if not node.has_modified:
                node.modified = read_node.modified
        serialized_data = S3Reader.serialize(node)
        self._storage.write(node.path, serialized_data)
        return OpResult.SUCCESS

    def delete(self, node: Node):
        self._storage.delete(node.path)
        return OpResult.SUCCESS

    def errorSupplier(self):
        return self._storage.errorSupplier