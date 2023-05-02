import base64
import hashlib
import logging
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Dict, List, Optional, Set, Type

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import EpochCounter, SystemCounter, Version
from faaskeeper.watch import WatchEventType
from functions.aws.model import SystemStorage
from functions.aws.model.system_storage import Node as SystemNode
from functions.aws.model.user_storage import Storage as UserStorage


class DistributorEventType(IntEnum):
    CREATE_NODE = 0
    SET_DATA = 1
    DELETE_NODE = 2


class DistributorEvent(ABC):
    def __init__(self, event_id: str, session_id: str, lock_timestamp: int):
        self._session_id = session_id
        self._event_id = event_id
        self._lock_timestamp = lock_timestamp

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def event_id(self) -> str:
        return self._event_id

    @property
    def lock_timestamp(self) -> int:
        return self._lock_timestamp

    @abstractmethod
    def serialize(self, serializer, base64_encoded=True) -> dict:
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
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:
        pass

    @abstractmethod
    def epoch_counters(self) -> List[str]:
        pass

    @abstractmethod
    def set_system_counter(self, system_counter: SystemCounter):
        pass


class DistributorCreateNode(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(
        self,
        event_id: str,
        session_id: str,
        lock_timestamp: int,
        parent_lock_timestamp: int,
        node: Node,
        parent_node: Node,
    ):
        super().__init__(event_id, session_id, lock_timestamp)
        self._node = node
        self._parent_node = parent_node
        self._parent_lock_timestamp = parent_lock_timestamp

    def serialize(self, serializer, base64_encoded=True) -> dict:
        """We must use JSON.
        IP and port are already serialized.
        """

        # FIXME: unify serialization - proper binary type for b64-encoded
        # FIXME: dynamodb vs sqs serialization
        data = {
            "type": serializer.serialize(self.type.value),
            "session_id": serializer.serialize(self.session_id),
            "event_id": serializer.serialize(self.event_id),
            "lock_timestamp": serializer.serialize(self.lock_timestamp),
            "parent_lock_timestamp": serializer.serialize(self._parent_lock_timestamp),
            "path": serializer.serialize(self.node.path),
            "parent_path": serializer.serialize(self.parent.path),
            "parent_children": serializer.serialize(self.parent.children),
        }

        if base64_encoded:
            data["data"] = {"B": self.node.data_b64}
        else:
            data["data"] = {"B": base64.b64decode(self.node.data_b64)}

        return data

    @staticmethod
    def deserialize(event_data: dict):

        deserializer = DistributorCreateNode._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))
        # counter = SystemCounter.from_provider_schema(event_data["counter"])
        # node.created = Version(counter, None)
        # node.modified = Version(counter, None)
        node.children = []
        # node.data = base64.b64decode(deserializer.deserialize(event_data["data"]))
        # node.data = base64.b64decode(event_data["data"]["B"])
        node.data_b64 = event_data["data"]["B"]

        parent_node = Node(deserializer.deserialize(event_data["parent_path"]))
        parent_node.children = deserializer.deserialize(event_data["parent_children"])

        session_id = deserializer.deserialize(event_data["session_id"])
        event_id = deserializer.deserialize(event_data["event_id"])
        lock_timestamp = deserializer.deserialize(event_data["lock_timestamp"])
        parent_lock_timestamp = deserializer.deserialize(
            event_data["parent_lock_timestamp"]
        )

        return DistributorCreateNode(
            event_id,
            session_id,
            lock_timestamp,
            parent_lock_timestamp,
            node,
            parent_node,
        )

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        system_node = system_storage.read_node(self.node)

        if system_node.Status == SystemNode.Status.NOT_EXISTS:
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": f"node {self.node.path} does not exist in system storage",
            }

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) > 0
            and system_node.pending_updates[0] != self.event_id
        ):

            if (
                system_node.Status == SystemNode.Status.LOCKED
                and system_node.lock.timestamp == self.lock_timestamp
            ):
                # Now we try to commit the node, but only if we still own a lock.
                # This is equivalent to a CAS.

                status = system_storage.commit_nodes(
                    [
                        system_storage.generate_commit_node(
                            self._node,
                            self.lock_timestamp,
                            set(
                                [
                                    NodeDataType.CREATED,
                                    NodeDataType.MODIFIED,
                                    NodeDataType.CHILDREN,
                                ]
                            ),
                            self.event_id,
                        ),
                        system_storage.generate_commit_node(
                            self._parent_node,
                            self._parent_lock_timestamp,
                            set([NodeDataType.CHILDREN]),
                        ),
                    ],
                )
                if status:
                    print("Committed the node!")
                else:
                    logging.error("Failing to apply the update - node still locked")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }
            else:
                return {
                    "status": "failure",
                    "path": self.node.path,
                    "reason": "update_not_committed",
                }

        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.write(self.node)
        # FIXME: update parent epoch and pxid
        # self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))

        system_storage.pop_pending_update(system_node.node)

        return {
            "status": "success",
            "path": self.node.path,
            "system_counter": self.node.created.system.serialize(),
        }

    def epoch_counters(self) -> List[str]:
        # FIXME:
        return []

    def set_system_counter(self, system_counter: SystemCounter):

        # FIXME: parent counter
        self.node.created = Version(system_counter, None)
        self.node.modified = Version(system_counter, None)

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

    def __init__(self, event_id: str, session_id: str, lock_timestamp: int, node: Node):
        super().__init__(event_id, session_id, lock_timestamp)
        self._node = node

    def serialize(self, serializer, base64_encoded=True) -> dict:
        """We must use JSON.
        IP and port are already serialized.
        """
        data = {
            "type": serializer.serialize(self.type.value),
            "session_id": serializer.serialize(self.session_id),
            "event_id": serializer.serialize(self.event_id),
            "lock_timestamp": serializer.serialize(self.lock_timestamp),
            "path": serializer.serialize(self.node.path),
            "counter": self.node.modified.system.version,
        }

        if base64_encoded:
            data["data"] = {"B": self.node.data_b64}
        else:
            data["data"] = {"B": base64.b64decode(self.node.data_b64)}

        return data

    @staticmethod
    def deserialize(event_data: dict):

        deserializer = DistributorSetData._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))
        counter = SystemCounter.from_provider_schema(event_data["counter"])
        node.modified = Version(counter, None)
        # node.data = base64.b64decode(deserializer.deserialize(event_data["data"]))
        # node.data = base64.b64decode(event_data["data"]["B"])
        node.data_b64 = event_data["data"]["B"]

        session_id = deserializer.deserialize(event_data["session_id"])
        event_id = deserializer.deserialize(event_data["event_id"])
        lock_timestamp = deserializer.deserialize(event_data["lock_timestamp"])

        return DistributorSetData(event_id, session_id, lock_timestamp, node)

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        system_node = system_storage.read_node(self.node)

        if system_node.Status == SystemNode.Status.NOT_EXISTS:
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": f"node {self.node.path} does not exist in system storage",
            }

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) > 0
            and system_node.pending_updates[0] != self.event_id
        ):

            if (
                system_node.Status == SystemNode.Status.LOCKED
                and system_node.lock.timestamp == self.lock_timestamp
            ):
                status = system_storage.commit_node(
                    self.node,
                    self.lock_timestamp,
                    set([NodeDataType.MODIFIED]),
                    self.event_id,
                )
                if status:
                    print("Committed the node!")
                else:
                    logging.error("Failing to apply the update - node still locked")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }
            else:
                return {
                    "status": "failure",
                    "path": self.node.path,
                    "reason": "update_not_committed",
                }

        """
        On DynamoDB we skip updating the created version as it doesn't change.
        On S3, we need to write this every single time.
        """
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.node, set([NodeDataType.MODIFIED, NodeDataType.DATA]))

        system_storage.pop_pending_update(system_node.node)

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

    def epoch_counters(self) -> List[str]:

        hashed_path = hashlib.md5(self.node.path.encode()).hexdigest()
        return [
            f"{hashed_path}_{WatchEventType.NODE_DATA_CHANGED.value}"
            f"_{self.node.modified.system.sum}"
        ]

    def set_system_counter(self, system_counter: SystemCounter):

        self.node.modified = Version(system_counter, None)


class DistributorDeleteNode(DistributorEvent):

    _type_deserializer = TypeDeserializer()

    def __init__(
        self,
        event_id: str,
        session_id: str,
        lock_timestamp: int,
        parent_lock_timestamp: int,
        node: Node,
        parent_node: Node,
    ):
        super().__init__(event_id, session_id, lock_timestamp)
        self._node = node
        self._parent_node = parent_node
        self._parent_lock_timestamp = parent_lock_timestamp

    def serialize(self, serializer, base64_encoded=True) -> dict:
        """We must use JSON.
        IP and port are already serialized.
        """
        return {
            "type": serializer.serialize(self.type.value),
            "session_id": serializer.serialize(self.session_id),
            "event_id": serializer.serialize(self.event_id),
            "lock_timestamp": serializer.serialize(self.lock_timestamp),
            "parent_lock_timestamp": serializer.serialize(self._parent_lock_timestamp),
            "path": serializer.serialize(self.node.path),
            "parent_path": serializer.serialize(self.parent.path),
            "parent_children": serializer.serialize(self.parent.children),
        }

    @staticmethod
    def deserialize(event_data: dict):

        # FIXME: custom deserializer

        deserializer = DistributorCreateNode._type_deserializer
        node = Node(deserializer.deserialize(event_data["path"]))

        parent_node = Node(deserializer.deserialize(event_data["parent_path"]))
        parent_node.children = deserializer.deserialize(event_data["parent_children"])

        session_id = deserializer.deserialize(event_data["session_id"])
        event_id = deserializer.deserialize(event_data["event_id"])
        lock_timestamp = deserializer.deserialize(event_data["lock_timestamp"])
        parent_lock_timestamp = deserializer.deserialize(
            event_data["parent_lock_timestamp"]
        )

        return DistributorDeleteNode(
            event_id,
            session_id,
            lock_timestamp,
            parent_lock_timestamp,
            node,
            parent_node,
        )

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        system_node = system_storage.read_node(self.node)

        if system_node.Status == SystemNode.Status.NOT_EXISTS:
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": f"node {self.node.path} does not exist in system storage",
            }

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) > 0
            and system_node.pending_updates[0] != self.event_id
        ):

            if (
                system_node.Status == SystemNode.Status.LOCKED
                and system_node.lock.timestamp == self.lock_timestamp
            ):
                status = system_storage.commit_nodes(
                    [
                        system_storage.generate_commit_node(
                            self._parent_node,
                            self._parent_lock_timestamp,
                            set([NodeDataType.CHILDREN]),
                        )
                    ],
                    [
                        system_storage.generate_delete_node(
                            self.node, self.lock_timestamp, self.event_id
                        )
                    ],
                )
                if status:
                    print("Committed the node!")
                else:
                    logging.error("Failing to apply the update - node still locked")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }
            else:
                return {
                    "status": "failure",
                    "path": self.node.path,
                    "reason": "update_not_committed",
                }

        # FIXME: update
        # FIXME: retain the node to keep counters
        # self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.delete(self.node)
        # self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))

        system_storage.pop_pending_update(system_node.node)

        return {
            "status": "success",
            "path": self.node.path,
        }

    def epoch_counters(self) -> List[str]:
        # FIXME:
        return []

    def set_system_counter(self, system_counter: SystemCounter):
        # FIXME: parent counter
        pass

    @property
    def node(self) -> Node:
        return self._node

    @property
    def parent(self) -> Node:
        return self._parent_node

    @property
    def type(self) -> DistributorEventType:
        return DistributorEventType.DELETE_NODE


def builder(
    counter: SystemCounter, event_type: DistributorEventType, event: dict
) -> DistributorEvent:

    ops: Dict[DistributorEventType, Type[DistributorEvent]] = {
        DistributorEventType.CREATE_NODE: DistributorCreateNode,
        DistributorEventType.SET_DATA: DistributorSetData,
        DistributorEventType.DELETE_NODE: DistributorDeleteNode,
    }

    if event_type not in ops:
        raise NotImplementedError()

    distr_event = ops[event_type]
    op = distr_event.deserialize(event)
    op.set_system_counter(counter)

    return op
