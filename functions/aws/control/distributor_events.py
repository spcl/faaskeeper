import base64
from abc import ABC, abstractmethod
from enum import IntEnum

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import SystemCounter, Version

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
    def deserialize(event_data: dict) -> "DistributorEvent":
        pass

    @property
    @abstractmethod
    def type(self) -> DistributorEventType:
        pass

    @abstractmethod
    def execute(self, user_storage: UserStorage):
        pass


class DistributorCreateNode(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(self, node: Node, parent_node: Node):
        self._node = node
        self._parent_node = parent_node

    def serialize(self, serializer) -> dict:
        """We must use JSON.
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
    def deserialize(event_data: dict) -> "DistributorCreateNode":

        deserializer = DistributorCreateNode._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))
        counter = SystemCounter.from_provider_schema(event_data["counter"])
        node.created = Version(counter, None)
        node.modified = Version(counter, None)
        node.children = []
        node.data = base64.b64decode(deserializer.deserialize(event_data["data"]))
        print(node)

        parent_node = Node(deserializer.deserialize(event_data["parent_path"]))
        parent_node.children = deserializer.deserialize(event_data["parent_children"])
        print(parent_node)

        return DistributorCreateNode(node, parent_node)

    def execute(self, user_storage: UserStorage):

        user_storage.write(self.node)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))

    @property
    def node(self) -> Node:
        return self._node

    @property
    def parent(self) -> Node:
        return self._parent_node

    @property
    def type(self) -> DistributorEventType:
        return DistributorEventType.CREATE_NODE
