import base64
import hashlib
import logging
import time
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Dict, List, Optional, Set, Type

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import EpochCounter, SystemCounter, Version
from faaskeeper.watch import WatchEventType, WatchType
from functions.aws.model import SystemStorage
from functions.aws.model.watches import Watches 
from functions.aws.model.system_storage import Node as SystemNode
from functions.aws.model.user_storage import Storage as UserStorage
from functions.aws.stats import TimingStatistics
from functions.aws.model.watches import Watches

class DistributorEventType(IntEnum):
    CREATE_NODE = 0
    SET_DATA = 1
    DELETE_NODE = 2


class TriBool(IntEnum):
    CORRECT = 0
    LOCKED = 1
    INCORRECT = 2


class DistributorEvent(ABC):
    def __init__(self, event_id: str, session_id: str, lock_timestamp: int):
        self._session_id = session_id
        self._event_id = event_id
        self._lock_timestamp = lock_timestamp
        from functions.aws.config import Config

        self._config = Config.instance(False)
        self._timing_stats = TimingStatistics.instance()

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
    def generate_watches_event(self, region_watches: Watches) -> List[Watches.Watch_Event]:
        '''
        [watchType, path, watchDetails, node_timestamp]
        '''
        pass

    @abstractmethod
    def update_epoch_counters(self, user_storage: UserStorage, epoch_counters: Set[str]):
        '''
        store the epoch counters to the EXISTING node (and parent node) in the user storage.
        '''
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

    def _node_status(self, system_node: SystemNode) -> TriBool:

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) == 0
            or system_node.pending_updates[0] != self.event_id
        ):
            if system_node.locked and system_node.lock.timestamp == self.lock_timestamp:
                return TriBool.LOCKED
            else:
                return TriBool.INCORRECT
        else:
            return TriBool.CORRECT

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        system_node = system_storage.read_node(self.node)
        print("distributor system node", system_node)
        status = self._node_status(system_node)

        if status == TriBool.INCORRECT:
            logging.error("Failing to apply the update - node updated by someone else")
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": "update_not_committed",
            }

        elif status == TriBool.LOCKED:
            logging.error("Failing to apply the update - node still locked")

            transaction_status, old_nodes = system_storage.commit_nodes(
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
                return_old_on_failure=[self._node, self._parent_node],
            )
            # Transaction failed, let's verify that
            if not transaction_status and len(old_nodes) > 0:

                if self._node_status(old_nodes[0]) != TriBool.CORRECT:
                    logging.error("Failing to apply the update - couldn't commit")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }

        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.write(self.node)
        # FIXME: update parent pxid
        self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
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
        self.node.created = Version(system_counter, None)
        self.node.modified = Version(system_counter, None)

        self.parent.modified = Version(system_counter, None)

    def generate_watches_event(self, region_watches: Watches) -> List[Watches.Watch_Event]:
        # Query watches from DynaamoDB to decide later, if we should actually invoke call function, then we abstract if statement

        # if they should even be scheduled.
        all_watches = []

        all_watches += region_watches.query_watches(
            self.node.path, [WatchType.EXISTS]
        )

        all_watches += region_watches.query_watches(
            self.parent.path, [WatchType.GET_CHILDREN]
        )

        for idx, watch_entity in enumerate(all_watches):
            # assign node or parent node timestamp to the results
            if watch_entity[1] == self.node.path:
                all_watches[idx] = Watches.Watch_Event(WatchEventType.NODE_CREATED.value, watch_entity[0].value, watch_entity[1], self.node.modified.system.sum)
            elif watch_entity[1] == self.parent.path:
                all_watches[idx] = Watches.Watch_Event(WatchEventType.NODE_CHILDREN_CHANGED.value, watch_entity[0].value, watch_entity[1], self.parent.modified.system.sum)
        return all_watches

    def update_epoch_counters(self, user_storage: UserStorage, epoch_counters: Set[str]):
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.node)
        # FIXME: update pxid.
        self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
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

    def _node_status(self, system_node: SystemNode) -> TriBool:

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) == 0
            or system_node.pending_updates[0] != self.event_id
        ):
            if system_node.locked and system_node.lock.timestamp == self.lock_timestamp:
                return TriBool.LOCKED
            else:
                return TriBool.INCORRECT
        else:
            return TriBool.CORRECT

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        if self._config.benchmarking:
            begin_read = time.time()
        system_node = system_storage.read_node(self.node)

        status = self._node_status(system_node)

        if status == TriBool.INCORRECT:
            logging.error("Failing to apply the update - node updated by someone else")
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": "update_not_committed",
            }
        elif status == TriBool.LOCKED:

            logging.error("Failing to apply the update - node still locked")

            commit_status = system_storage.commit_node(
                self.node,
                self.lock_timestamp,
                set([NodeDataType.MODIFIED]),
                self.event_id,
            )
            # Transaction failed, let's verify that
            if not commit_status:

                # We shouldn't do a second read here.
                # Unfortunately, DynamoDB update-item returns the attributes only on a succesful
                # update. When it fails, we need to read manually.
                system_node = system_storage.read_node(self.node)

                if self._node_status(system_node) != TriBool.CORRECT:
                    logging.error("Failing to apply the update - couldn't commit")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }
        if self._config.benchmarking:
            end_read = time.time()
            self._timing_stats.add_result("exec_read", end_read - begin_read)

        """
        On DynamoDB we skip updating the created version as it doesn't change.
        On S3, we need to write this every single time.
        """
        if self._config.benchmarking:
            begin_write = time.time()
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.node, set([NodeDataType.MODIFIED, NodeDataType.DATA]))
        if self._config.benchmarking:
            end_write = time.time()
            self._timing_stats.add_result("exec_update", end_write - begin_write)

        if self._config.benchmarking:
            begin_pop = time.time()
        system_storage.pop_pending_update(system_node.node)
        if self._config.benchmarking:
            end_pop = time.time()
            self._timing_stats.add_result("exec_pop_updates", end_pop - begin_pop)

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

    def generate_watches_event(self, region_watches: Watches) -> List[Watches.Watch_Event]:
        all_watches = []
        all_watches += region_watches.query_watches(
            self.node.path, [WatchType.GET_DATA, WatchType.EXISTS]
        )

        for idx, watch_entity in enumerate(all_watches):
            all_watches[idx] = Watches.Watch_Event(WatchEventType.NODE_DATA_CHANGED.value, watch_entity[0].value, watch_entity[1], self.node.modified.system.sum)

        return all_watches

    def update_epoch_counters(self, user_storage: UserStorage, epoch_counters: Set[str]):
        self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.node, set([NodeDataType.MODIFIED, NodeDataType.DATA]))

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

    def _node_status(self, system_node: SystemNode) -> TriBool:

        # The node is no longer locked, but the update is not there
        if (
            len(system_node.pending_updates) == 0
            or system_node.pending_updates[0] != self.event_id
        ):
            if system_node.locked and system_node.lock.timestamp == self.lock_timestamp:
                return TriBool.LOCKED
            else:
                return TriBool.INCORRECT
        else:
            return TriBool.CORRECT

    def execute(
        self,
        system_storage: SystemStorage,
        user_storage: UserStorage,
        epoch_counters: Set[str],
    ) -> Optional[dict]:

        system_node = system_storage.read_node(self.node)

        # TODO: in the future, we want to allow reader-writer locks on the parent node.
        # Then, for deletion, it means that we need to search the loop for the pending update
        # as we ne longer have the guarantee that our update is the first one.
        # The node is no longer locked, but the update is not there
        status = self._node_status(system_node)

        if status == TriBool.INCORRECT:
            logging.error("Failing to apply the update - node updated by someone else")
            return {
                "status": "failure",
                "path": self.node.path,
                "reason": "update_not_committed",
            }

        elif status == TriBool.LOCKED:
            logging.error("Failing to apply the update - node still locked")

            transaction_status, old_nodes = system_storage.commit_nodes(
                [
                    system_storage.generate_commit_node(
                        self._parent_node,
                        self._parent_lock_timestamp,
                        set([NodeDataType.CHILDREN]),
                    ),
                    system_storage.generate_delete_node(
                        self.node, self.lock_timestamp, self.event_id
                    ),
                ],
                return_old_on_failure=[self._parent_node, self._node],
            )
            # Transaction failed, let's verify that
            # if not transaction_status:
            if not transaction_status and len(old_nodes) > 0:

                if self._node_status(old_nodes[1]) != TriBool.CORRECT:
                    logging.error("Failing to apply the update - couldn't commit")
                    return {
                        "status": "failure",
                        "path": self.node.path,
                        "reason": "update_not_committed",
                    }

        # FIXME: update
        # FIXME: retain the node to keep counters
        # self.node.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.delete(self.node)
        self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
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
        # pass
        self.node.modified = Version(system_counter, None) # this is for notification use, have nothing to do w/ the node commit
        self.parent.modified = Version(system_counter, None)

    def generate_watches_event(self, region_watches: Watches) -> List[Watches.Watch_Event]:
        # Query watches from DynaamoDB to decide later, if we should actually invoke call function, then we abstract if statement

        # if they should even be scheduled.
        all_watches = []
        all_watches += region_watches.query_watches(
            self.node.path, [WatchType.EXISTS, WatchType.GET_DATA, WatchType.GET_CHILDREN]
        )

        all_watches += region_watches.query_watches(
            self.parent.path, [WatchType.GET_CHILDREN]
        )

        for idx, watch_entity in enumerate(all_watches):
            if watch_entity[1] == self.node.path:
                all_watches[idx] = Watches.Watch_Event(WatchEventType.NODE_DELETED.value, watch_entity[0].value, watch_entity[1], self.node.modified.system.sum)
            elif watch_entity[1] == self.parent.path:
                all_watches[idx] = Watches.Watch_Event(WatchEventType.NODE_CHILDREN_CHANGED.value, watch_entity[0].value, watch_entity[1], self.parent.modified.system.sum)

        return all_watches
    
    def update_epoch_counters(self, user_storage: UserStorage, epoch_counters: Set[str]):
        self.parent.modified.epoch = EpochCounter.from_raw_data(epoch_counters)
        user_storage.update(self.parent, set([NodeDataType.CHILDREN]))

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
    op:DistributorEvent = distr_event.deserialize(event)
    op.set_system_counter(counter)

    return op
