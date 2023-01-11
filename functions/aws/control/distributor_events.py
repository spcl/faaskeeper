import base64
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Optional, Set

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import EpochCounter, SystemCounter, Version

from ..model.user_storage import Storage as UserStorage


class DistributorEventType(IntEnum):
    CREATE_NODE = 0
    SET_DATA = 1
    DELETE_NODE = 2


class DistributorEvent(ABC):
    @abstractmethod
    def serialize(self, serializer) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def deserialize(event_data: dict):
        pass

    @property
    @abstractmethod
    def type(self) -> DistributorEventType:
        pass

    @property
    @abstractmethod
    def node(self) -> Node:
        pass

    @abstractmethod
    def execute(
        self, user_storage: UserStorage, epoch_counters: Set[str]
    ) -> Optional[dict]:
        pass


class DistributorCreateNode(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(self, node: Node, parent_node: Node):
        self._node = node
        self._parent_node = parent_node

    def serialize(self, serializer) -> dict:
        """We must use JSON.
            IP and port are already serialized.
        """

        return {
            "type": serializer.serialize(self.type.value),
            "path": serializer.serialize(self.node.path),
            "counter": self.node.created.system.version,
            "data": serializer.serialize(self.node.data),
            "parent_path": serializer.serialize(self.parent.path),
            "parent_children": serializer.serialize(self.parent.children),
        }

    @staticmethod
    def deserialize(event_data: dict):

        deserializer = DistributorCreateNode._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))
        counter = SystemCounter.from_provider_schema(event_data["counter"])
        node.created = Version(counter, None)
        node.modified = Version(counter, None)
        node.children = []
        # node.data = base64.b64decode(deserializer.deserialize(event_data["data"]))
        node.data = base64.b64decode(event_data["data"]["B"])

        parent_node = Node(deserializer.deserialize(event_data["parent_path"]))
        parent_node.children = deserializer.deserialize(event_data["parent_children"])

        return DistributorCreateNode(node, parent_node)

    def execute(
        self, user_storage: UserStorage, epoch_counters: Set[str]
    ) -> Optional[dict]:
        # FIXME: Update
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.write(self.node)
        # FIXME: update parent epoch and pxid
        # self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))
        return {
            "status": "success",
            "path": self.node.path,
            "system_counter": self.node.created.system.serialize(),
        }

    @property
    def node(self) -> Node:
        return self._node

    @property
    def parent(self) -> Node:
        return self._parent_node

    @property
    def type(self) -> DistributorEventType:
        return DistributorEventType.CREATE_NODE


class DistributorSetData(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(self, node: Node):
        self._node = node

    def serialize(self, serializer) -> dict:
        """We must use JSON.
            IP and port are already serialized.
        """
        return {
            "type": serializer.serialize(self.type.value),
            "path": serializer.serialize(self.node.path),
            "counter": self.node.modified.system.version,
            "data": serializer.serialize(self.node.data),
        }

    @staticmethod
    def deserialize(event_data: dict):

        deserializer = DistributorSetData._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))
        counter = SystemCounter.from_provider_schema(event_data["counter"])
        node.modified = Version(counter, None)
        # node.data = base64.b64decode(deserializer.deserialize(event_data["data"]))
        node.data = base64.b64decode(event_data["data"]["B"])

        return DistributorSetData(node)

    def execute(
        self, user_storage: UserStorage, epoch_counters: Set[str]
    ) -> Optional[dict]:
        # FIXME: update
        """
            On DynamoDB we skip updating the created version as it doesn't change.
            On S3, we need to write this every single time.
        """
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.node, set([NodeDataType.MODIFIED, NodeDataType.DATA]))
        return {
            "status": "success",
            "path": self.node.path,
            "modified_system_counter": self.node.modified.system.serialize(),
        }

    @property
    def node(self) -> Node:
        return self._node

    @property
    def type(self) -> DistributorEventType:
        return DistributorEventType.SET_DATA


class DistributorDeleteNode(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(self, node: Node, parent_node: Node):
        self._node = node
        self._parent_node = parent_node

    def serialize(self, serializer) -> dict:
        """We must use JSON.
            IP and port are already serialized.
        """
        return {
            "type": serializer.serialize(self.type.value),
            "path": serializer.serialize(self.node.path),
            "parent_path": serializer.serialize(self.parent.path),
            "parent_children": serializer.serialize(self.parent.children),
        }

    @staticmethod
    def deserialize(event_data: dict):

        deserializer = DistributorCreateNode._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))

        parent_node = Node(deserializer.deserialize(event_data["parent_path"]))
        parent_node.children = deserializer.deserialize(event_data["parent_children"])

        return DistributorDeleteNode(node, parent_node)

    def execute(
        self, user_storage: UserStorage, epoch_counters: Set[str]
    ) -> Optional[dict]:

        # FIXME: update
        # FIXME: retain the node to keep counters
        # self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.delete(self.node)
        # self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))
        return {
            "status": "success",
            "path": self.node.path,
        }

    @property
    def node(self) -> Node:
        return self._node

    @property
    def parent(self) -> Node:
        return self._parent_node

    @property
    def type(self) -> DistributorEventType:
        return DistributorEventType.DELETE_NODE
