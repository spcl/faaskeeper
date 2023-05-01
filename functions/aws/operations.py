import logging
import pathlib
import time
from abc import ABC, abstractmethod
from datetime import datetime
from time import sleep
from typing import Dict, Optional, Tuple, Type, cast

from faaskeeper.node import Node, NodeDataType
from faaskeeper.operations import (
    CreateNode,
    DeleteNode,
    DeregisterSession,
    RequestOperation,
    SetData,
)
from faaskeeper.version import Version
from functions.aws.control.channel import Client
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorDeleteNode,
    DistributorSetData,
)
from functions.aws.control.distributor_queue import DistributorQueue
from functions.aws.model import SystemStorage


class Executor(ABC):
    def __init__(self, op: RequestOperation):
        self._op = op

    @abstractmethod
    def lock_and_read(self, system_storage: SystemStorage) -> Tuple[bool, dict]:
        pass

    @abstractmethod
    def distributor_push(self, client: Client, distributor_queue: DistributorQueue):
        pass

    @abstractmethod
    def commit_and_unlock(self, system_storage: SystemStorage) -> Tuple[bool, dict]:
        pass


class CreateNodeExecutor(Executor):
    def __init__(self, op: CreateNode):
        super().__init__(op)

    @property
    def op(self) -> CreateNode:
        return cast(CreateNode, self._op)

    def lock_and_read(self, system_storage: SystemStorage) -> Tuple[bool, dict]:

        # TODO: ephemeral
        # TODO: sequential
        path = self.op.path
        logging.info(f"Attempting to create node at {path}")

        # FIXME :limit number of attempts
        while True:
            self._timestamp = int(datetime.now().timestamp())
            lock, node = system_storage.lock_node(path, self._timestamp)
            if not lock:
                sleep(2)
            else:
                break

        # does the node exist?
        if node is not None:
            system_storage.unlock_node(path, self._timestamp)
            return (False, {"status": "failure", "path": path, "reason": "node_exists"})

        # lock the parent - unless we're already at the root
        node_path = pathlib.Path(path)
        parent_path = node_path.parent.absolute()
        self._parent_timestamp: Optional[int] = None
        while True:
            self._parent_timestamp = int(datetime.now().timestamp())
            _, self._parent_node = system_storage.lock_node(
                str(parent_path), self._parent_timestamp
            )
            if not lock:
                sleep(1)
            else:
                break
        # does the node does not exist?
        if self._parent_node is None:
            system_storage.unlock_node(str(parent_path), self._parent_timestamp)
            system_storage.unlock_node(path, self._timestamp)
            return (
                False,
                {
                    "status": "failure",
                    "path": str(parent_path),
                    "reason": "node_doesnt_exist",
                },
            )

        return (True, {})

    def commit_and_unlock(self, system_storage: SystemStorage) -> Tuple[bool, dict]:

        assert self._parent_node
        assert self._parent_timestamp

        # FIXME: we shouldn't use writer ID anymore
        self._counter = system_storage.increase_system_counter(0)
        if self._counter is None:
            return (False, {"status": "failure", "reason": "unknown"})

        # store the created and the modified version counter
        self._node = Node(self.op.path)
        self._node.created = Version(self._counter, None)
        self._node.modified = Version(self._counter, None)
        self._node.children = []
        # we propagate data to another queue, we should use the already
        # base64-encoded data
        # FIXME: keep the information if base64 encoding is actually applied?
        # Important for Redis
        self._node.data_b64 = self.op.data_b64

        # FIXME: make both operations concurrently
        # unlock parent
        # parent now has one child more
        self._parent_node.children.append(pathlib.Path(self.op.path).name)
        system_storage.commit_node(
            self._parent_node, self._parent_timestamp, set([NodeDataType.CHILDREN])
        )
        # commit node
        system_storage.commit_node(
            self._node,
            self._timestamp,
            set([NodeDataType.CREATED, NodeDataType.MODIFIED, NodeDataType.CHILDREN]),
        )

        return (True, {})

    def distributor_push(self, client: Client, distributor_queue: DistributorQueue):

        assert self._counter
        assert self._parent_node
        distributor_queue.push(
            self._counter,
            DistributorCreateNode(client.session_id, self._node, self._parent_node),
            client,
        )


class DeregisterSessionExecutor(Executor):
    def __init__(self, op: DeregisterSession):
        super().__init__(op)

    @property
    def op(self) -> DeregisterSession:
        return cast(DeregisterSession, self._op)

    def lock_and_read(self, system_storage: SystemStorage) -> Tuple[bool, dict]:
        return (True, {})

    def distributor_push(self, client: Client, distributor_queue: DistributorQueue):
        pass

    def commit_and_unlock(self, system_storage: SystemStorage) -> Tuple[bool, dict]:

        # TODO: remove ephemeral nodes
        # FIXME: check return value
        session_id = self.op.session_id
        if system_storage.delete_user(session_id):
            return (True, {"status": "success", "session_id": session_id})
        else:
            logging.error(f"Attempting to remove non-existing user {session_id}")
            return (
                False,
                {
                    "status": "failure",
                    "session_id": session_id,
                    "reason": "session_does_not_exist",
                },
            )


class SetDataExecutor(Executor):
    def __init__(self, op: SetData):
        super().__init__(op)

    @property
    def op(self) -> SetData:
        return cast(SetData, self._op)

    def lock_and_read(self, system_storage: SystemStorage) -> Tuple[bool, dict]:

        path = self.op.path
        logging.info(f"Attempting to write data at {path}")

        begin_lock = time.time()
        # FIXME :limit number of attempts
        while True:
            self._timestamp = int(datetime.now().timestamp())
            lock, self._system_node = system_storage.lock_node(path, self._timestamp)
            if not lock:
                sleep(2)
            else:
                break
        end_lock = time.time()
        logging.info(f"Acquired lock at {path}")

        # does the node exist?
        if self._system_node is None:
            system_storage.unlock_node(path, self._timestamp)
            return (
                False,
                {"status": "failure", "path": path, "reason": "node_doesnt_exist"},
            )

        return (True, {})

    def distributor_push(self, client: Client, distributor_queue: DistributorQueue):

        assert self._counter
        assert self._system_node

        begin_push = time.time()

        assert distributor_queue
        print(f"start pushing update")
        distributor_queue.push(
            self._counter,
            DistributorSetData(client.session_id, self._system_node),
            client,
        )
        end_push = time.time()
        logging.info(f"Finished pushing update")

        end = time.time()

    def commit_and_unlock(self, system_storage: SystemStorage) -> Tuple[bool, dict]:

        assert self._system_node

        begin_atomic = time.time()
        # FIXME: we shouldn't use writer ID anymore
        self._counter = system_storage.increase_system_counter(0)
        if self._counter is None:
            return (False, {"status": "failure", "reason": "unknown"})
        end_atomic = time.time()
        logging.info(f"Incremented system counter")

        # store only the modified version counter
        # the new data will be written by the distributor
        self._system_node.modified = Version(self._counter, None)
        self._system_node.data_b64 = self.op.data_b64
        logging.info(f"Finished commit preparation")

        begin_commit = time.time()
        if not system_storage.commit_node(self._system_node, self._timestamp):
            return (False, {"status": "failure", "reason": "unknown"})
        end_commit = time.time()
        logging.info(f"Finished commit")

        # global repetitions
        # global sum_total
        # global sum_lock
        # global sum_atomic
        # global sum_commit
        # global sum_push
        # repetitions += 1
        # sum_total += end - begin
        # sum_lock += end_lock - begin_lock
        # sum_atomic += end_atomic - begin_atomic
        # sum_commit += end_commit - begin_commit
        # sum_push += end_push - begin_push
        # if repetitions % 100 == 0:
        #    print("RESULT_TOTAL", sum_total)
        #    print("RESULT_LOCK", sum_lock)
        #    print("RESULT_ATOMIC", sum_atomic)
        #    print("RESULT_COMMIT", sum_commit)
        #    print("RESULT_PUSH", sum_push)

        return (True, {})


def builder(
    operation: str, event_id: str, event: dict
) -> Tuple[Optional[Executor], dict]:

    ops: Dict[str, Tuple[Type[RequestOperation], Type[Executor]]] = {
        "create_node": (CreateNode, CreateNodeExecutor),
        "set_data": (SetData, SetDataExecutor),
        # "delete_node": (DeleteNode, DeleteNodeExecutor),
        "deregister_session": (DeregisterSession, DeregisterSessionExecutor),
    }

    if operation not in ops:
        logging.error(
            "Unknown operation {op} with ID {event_id}, "
            "timestamp {timestamp}".format(
                op=operation,
                event_id=event_id,
                timestamp=event["timestamp"],
            )
        )
        error_msg = {"status": "failure", "reason": "incorrect_request"}
        return (None, error_msg)

    operation_type, executor_type = ops[operation]
    op = operation_type.deserialize(event)
    if op is None:
        logging.error(
            "Incorrect event with ID {id}, timestamp {timestamp}".format(
                id=event_id, timestamp=event["timestamp"]
            )
        )
        error_msg = {"status": "failure", "reason": "incorrect_request"}
        return (None, error_msg)

    return (executor_type(op), {})
