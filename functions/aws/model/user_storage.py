import struct
from abc import ABC, abstractmethod
from enum import Enum
from typing import Set

from boto3.dynamodb.types import TypeSerializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.providers.serialization import S3Reader
from functions.aws.control import DynamoStorage as DynamoDriver
from functions.aws.control import S3Storage as S3Driver


class OpResult(Enum):
    SUCCESS = 0
    NODE_EXISTS = 1
    NODE_DOESNT_EXIST = 2


class Storage(ABC):
    def __init__(self, storage_name: str):
        self._storage_name = storage_name

    @abstractmethod
    def write(self, node: Node):
        """
            Write object or set of values to the storage.
        """
        pass

    @abstractmethod
    def update(self, node: Node, updates: Set[NodeDataType] = set()):
        """
            Update existing object or set of values in the storage.
        """
        pass

    @abstractmethod
    def delete(self, node: Node):
        """
           Remove contents stored in the object/row in the storage.
        """
        pass

    @property
    @abstractmethod
    def errorSupplier(self):
        pass


class DynamoStorage(Storage):
    def _toSchema(self, node: Node, updates: Set[NodeDataType] = set()):
        schema: dict = {}
        # FIXME: pass epoch counter value
        if NodeDataType.DATA in updates:
            schema = {**schema, "data": {"B": node.data}}
        if NodeDataType.CREATED in updates:
            schema = {
                **schema,
                "cFxidSys": node.created.system.version,
                "cFxidEpoch": {"NS": ["0"]},
            }
        if NodeDataType.MODIFIED in updates:
            schema = {
                **schema,
                "mFxidSys": node.modified.system.version,
                "mFxidEpoch": {"NS": ["0"]},
            }
        if NodeDataType.CHILDREN in updates:
            schema = {
                **schema,
                "children": self._type_serializer.serialize(node.children),
            }
        return schema

    def __init__(self, table_name: str):
        self._storage = DynamoDriver(table_name, "path")
        self._type_serializer = TypeSerializer()

    def write(self, node: Node):
        try:
            self._storage.write(
                node.path,
                {
                    "path": {"S": node.path},
                    **self._toSchema(
                        node,
                        set(
                            [
                                NodeDataType.CREATED,
                                NodeDataType.MODIFIED,
                                NodeDataType.DATA,
                                NodeDataType.CHILDREN,
                            ]
                        ),
                    ),
                },
            )
            return OpResult.SUCCESS
        except self.errorSupplier.ConditionalCheckFailedException:
            return OpResult.NODE_EXISTS

    def update(self, node: Node, updates: Set[NodeDataType] = set()):
        try:
            self._storage.update_node(node, updates)
            return OpResult.SUCCESS
        except self.errorSupplier.ConditionalCheckFailedException:
            return OpResult.NODE_DOESNT_EXIST

    def delete(self, node: Node):
        self._storage.delete(node.path)

    @property
    def errorSupplier(self):
        return self._storage.errorSupplier


class S3Storage:
    def __init__(self, bucket_name: str):
        self._storage = S3Driver(bucket_name)

    def write(self, node: Node):
        self._storage.write(node.path, S3Reader.serialize(node))
        return OpResult.SUCCESS

    def update(self, node: Node, updates: Set[NodeDataType] = set()):
        # we need to download the data from storage
        # FIXME: this should be in distributor
        if not node.has_data:
            # FIXME: add this to the library somehow
            node_data = self._storage.read(node.path)
            header_size = struct.unpack_from("I", node_data)[0]
            node.data = node_data[header_size:]
        self._storage.write(node.path, S3Reader.serialize(node))
        return OpResult.SUCCESS

    def delete(self, node: Node):
        self._storage.delete(node.path)

    @property
    def errorSupplier(self):
        return self._storage.errorSupplier
