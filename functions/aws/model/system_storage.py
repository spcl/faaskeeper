from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum
from typing import List, Optional, Set, Tuple

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

# from faaskeeper.node import Node, NodeDataType
import faaskeeper.node
from faaskeeper.stats import StorageStatistics
from faaskeeper.version import SystemCounter, Version
from functions.aws.control.dynamo import DynamoStorage as DynamoDriver


class Node:
    class Status(Enum):
        EXISTS = (0,)
        NOT_EXISTS = (1,)
        LOCKED = 2

    Lock = namedtuple("Lock", ["timestamp"])

    def __init__(self, node: faaskeeper.node.Node, status: Status):
        self._node = node
        self._status = status
        self._pending_updates: List[str] = []

    @property
    def lock(self) -> Optional["Node.Lock"]:
        return self._lock

    @lock.setter
    def lock(self, timestamp: str):
        self._lock = Node.Lock(timestamp)

    @property
    def status(self) -> "Status":
        return self._status

    @property
    def node(self) -> faaskeeper.node.Node:
        return self._node


class Storage(ABC):
    @property
    def lock_lifetime(self) -> int:
        """
        Clients are allowed to hold the lock for no more than 5 seconds.
        We add 2 seconds to account for clock drift of max 1 second.
        """
        return 7

    @abstractmethod
    def delete_user(self, session_id: str):
        """
        Remove contents stored in the object/row in the storage.
        """
        pass

    @abstractmethod
    def lock_node(
        self, path: str, timestamp: int
    ) -> Tuple[bool, Optional[faaskeeper.node.Node]]:
        pass

    @abstractmethod
    def unlock_node(self, path: str, timestamp: int):
        pass

    @abstractmethod
    def generate_delete_node(self, node: faaskeeper.node.Node, timestamp: int) -> dict:
        pass

    @abstractmethod
    def delete_node(self, node: faaskeeper.node.Node, timestamp: int):
        pass

    @abstractmethod
    def commit_node(
        self,
        node: faaskeeper.node.Node,
        timestamp: int,
        updates: Set[faaskeeper.node.NodeDataType] = set(),
    ) -> bool:
        pass

    @abstractmethod
    def generate_commit_node(
        self,
        node: faaskeeper.node.Node,
        timestamp: int,
        updates: Set[faaskeeper.node.NodeDataType] = set(),
    ) -> dict:
        pass

    @abstractmethod
    def commit_nodes(self, updates: List[dict], deletions: List[dict] = []) -> bool:
        pass

    @abstractmethod
    def increase_system_counter(self, writer_id: int) -> Optional[SystemCounter]:
        pass

    @abstractmethod
    def read_node(self, node: faaskeeper.node.Node) -> Node:
        pass


class DynamoStorage(Storage):

    # FIXME: implement counter increase
    # FIXME: implement epoch counter change

    def __init__(self, storage_name: str):
        self._users_storage = DynamoDriver(f"{storage_name}-users", "user")
        self._state_storage = DynamoDriver(f"{storage_name}-state", "path")
        self._type_serializer = TypeSerializer()
        self._type_deserializer = TypeDeserializer()

    def delete_user(self, session_id: str) -> bool:
        try:
            self._users_storage.delete(session_id)
            return True
        except self._users_storage.errorSupplier.ConditionalCheckFailedException:
            return False

    def lock_node(
        self, path: str, timestamp: int
    ) -> Tuple[bool, Optional[faaskeeper.node.Node]]:

        # FIXME: move this to the interface of control driver
        # we set the timelock value to the timestamp
        # for comparison, we subtract from the timestamp the maximum lock holding time
        try:
            ret = self._state_storage._dynamodb.update_item(
                TableName=self._state_storage.storage_name,
                # path to the node
                Key={"path": {"S": path}},
                # create timelock
                UpdateExpression="SET timelock = :newlockvalue",
                # lock doesn't exist or it's already expired
                ConditionExpression="(attribute_not_exists(timelock)) or "
                "(timelock < :newlockshifted)",
                # timelock value
                ExpressionAttributeValues={
                    ":newlockvalue": {"N": str(timestamp)},
                    ":newlockshifted": {"N": str(timestamp - self.lock_lifetime)},
                },
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )
            StorageStatistics.instance().add_write_units(
                ret["ConsumedCapacity"]["CapacityUnits"]
            )
            # store raw provider data
            data = ret["Attributes"]
            n: Optional[faaskeeper.node.Node] = None
            # FIXME: merge with library code?
            # node already exists
            if "cFxidSys" in data:
                n = faaskeeper.node.Node(path)
                created = SystemCounter.from_provider_schema(
                    data["cFxidSys"]  # type: ignore
                )
                n.created = Version(
                    created,
                    None
                    # EpochCounter.from_provider_schema(data["cFxidEpoch"]),
                )
                modified = SystemCounter.from_provider_schema(
                    data["mFxidSys"]  # type: ignore
                )
                n.modified = Version(
                    modified,
                    None
                    # EpochCounter.from_provider_schema(data["mFxidEpoch"]),
                )
                n.children = self._type_deserializer.deserialize(data["children"])
            return (True, n)
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return (False, None)

    def unlock_node(self, path: str, timestamp: int) -> bool:
        """
        We need to make sure that we're still the ones holding a timelock.
        Then, we need to remove the timelock.

        We don't perform any additional updates - just unlock.
        """
        return self.commit_node(faaskeeper.node.Node(path), timestamp)

    def generate_commit_node(
        self,
        node: faaskeeper.node.Node,
        timestamp: int,
        updates: Set[faaskeeper.node.NodeDataType] = set(),
    ) -> dict:

        # we always commit the modified stamp
        update_expr = "REMOVE timelock "
        if len(updates):
            update_expr = f"{update_expr} SET "
        update_values = {}

        if faaskeeper.node.NodeDataType.CREATED in updates:
            update_expr = f"{update_expr} cFxidSys = :createdStamp,"
            update_values[":createdStamp"] = node.created.system.version
        if faaskeeper.node.NodeDataType.MODIFIED in updates:
            update_expr = f"{update_expr} mFxidSys = :modifiedStamp,"
            update_values[":modifiedStamp"] = node.modified.system.version
        if faaskeeper.node.NodeDataType.CHILDREN in updates:
            update_expr = f"{update_expr} children = :children,"
            update_values[":children"] = self._type_serializer.serialize(  # type: ignore
                node.children  # type: ignore
            )
        # strip traling comma - boto3 will not accept that
        update_expr = update_expr[:-1]

        return {
            "TableName": self._state_storage.storage_name,
            # path to the node
            "Key": {"path": {"S": node.path}},
            # create timelock
            "UpdateExpression": update_expr,
            # lock doesn't exist or it's already expired
            "ConditionExpression": "(attribute_exists(timelock)) "
            "and (timelock = :mytimelock)",
            # timelock value
            "ExpressionAttributeValues": {
                ":mytimelock": {"N": str(timestamp)},
                **update_values,
            },
        }

    def commit_node(
        self,
        node: faaskeeper.node.Node,
        timestamp: int,
        updates: Set[faaskeeper.node.NodeDataType] = set(),
    ) -> bool:

        """
        We need to make sure that we're still the ones holding a timelock.
        Then, we need to remove the timelock and update counters.

        Depending on the usage, we always modify the modified timestamp (modify),
        but we might also store the created timestamp (create node).
        """

        # FIXME: move this to the interface of control driver
        try:

            # https://github.com/python/mypy/issues/6799
            ret = self._state_storage._dynamodb.update_item(  # type: ignore
                **self.generate_commit_node(node, timestamp, updates),
                ReturnConsumedCapacity="TOTAL",  # type: ignore
            )
            StorageStatistics.instance().add_write_units(
                ret["ConsumedCapacity"]["CapacityUnits"]
            )
            return True
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return False

    def commit_nodes(self, updates: List[dict], deletions: List[dict] = []) -> bool:

        try:
            transaction_items = []

            for update in updates:
                transaction_items.append({"Update": update})

            for deletion in deletions:
                transaction_items.append({"Delete": deletion})

            ret = self._state_storage._dynamodb.transact_write_items(
                TransactItems=transaction_items, ReturnConsumedCapacity="TOTAL"  # type: ignore
            )

            for table in ret["ConsumedCapacity"]:
                StorageStatistics.instance().add_write_units(
                    table["WriteCapacityUnits"]
                )
                if "ReadCapacityUnits" in table:
                    StorageStatistics.instance().add_read_units(
                        table["ReadCapacityUnits"]  # type: ignore
                    )
            return True
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return False

    def increase_system_counter(self, writer_id: int) -> Optional[SystemCounter]:

        try:
            ret = self._state_storage._dynamodb.update_item(
                TableName=self._state_storage.storage_name,
                # path to the node
                Key={"path": {"S": "fxid"}},
                # add '1' to counter at given position
                UpdateExpression=f"ADD #D[{writer_id}] :inc",
                ExpressionAttributeNames={"#D": "cFxidSys"},
                ExpressionAttributeValues={":inc": {"N": "1"}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )

            StorageStatistics.instance().add_write_units(
                ret["ConsumedCapacity"]["CapacityUnits"]
            )
            return SystemCounter.from_provider_schema(
                ret["Attributes"]["cFxidSys"]  # type: ignore
            )
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return None

    def generate_delete_node(self, node: faaskeeper.node.Node, timestamp: int) -> dict:

        return {
            "TableName": self._state_storage.storage_name,
            # path to the node
            "Key": {"path": {"S": node.path}},
            # lock exists and it's ours
            "ConditionExpression": "(attribute_exists(timelock)) "
            "and (timelock = :mytimelock)",
            # timelock value
            "ExpressionAttributeValues": {":mytimelock": {"N": str(timestamp)}},
        }

    def delete_node(self, node: faaskeeper.node.Node, timestamp: int):

        # https://github.com/python/mypy/issues/6799
        ret = self._state_storage._dynamodb.delete_item(  # type: ignore
            **self.generate_delete_node(node, timestamp),
            ReturnConsumedCapacity="TOTAL",
        )
        StorageStatistics.instance().add_write_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

    def read_node(self, node: faaskeeper.node.Node) -> Node:

        ret = self._state_storage._dynamodb.get_item(
            TableName=self._state_storage.storage_name,
            # path to the node
            Key={"path": {"S": node.path}},
            ReturnConsumedCapacity="TOTAL",
        )
        StorageStatistics.instance().add_read_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

        if "Item" not in ret:
            return Node(node, Node.Status.NOT_EXISTS)

        data = ret["Item"]
        # is it still locked?
        dynamo_node: Node
        if "timelock" in data:
            dynamo_node = Node(node, Node.Status.LOCKED)
            # FIXE: lock details
        else:
            dynamo_node = Node(node, Node.Status.EXISTS)

        # FIXME: parse list updates
        created = SystemCounter.from_provider_schema(data["cFxidSys"])  # type: ignore
        dynamo_node.node.created = Version(
            created,
            None
            # EpochCounter.from_provider_schema(data["cFxidEpoch"]),
        )
        modified = SystemCounter.from_provider_schema(data["mFxidSys"])  # type: ignore
        dynamo_node.node.modified = Version(modified, None)
        dynamo_node.node.children = self._type_deserializer.deserialize(
            data["children"]
        )
        return dynamo_node
