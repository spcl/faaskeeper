from abc import ABC, abstractmethod
from typing import Optional, Tuple

from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version
from functions.aws.control import DynamoStorage as DynamoDriver


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
    def lock_node(self, path: str, timestamp: int) -> Tuple[bool, Optional[Node]]:
        pass

    @abstractmethod
    def commit_node(self, node: Node, timestamp: int) -> bool:
        pass

    @abstractmethod
    def increase_system_counter(self, writer_id: int) -> Optional[SystemCounter]:
        pass


class DynamoStorage(Storage):

    # FIXME: implement counter increase
    # FIXME: implement epoch counter change

    def __init__(self, storage_name: str):
        self._users_storage = DynamoDriver(f"{storage_name}-users", "user")
        self._state_storage = DynamoDriver(f"{storage_name}-state", "path")

    def delete_user(self, session_id: str) -> bool:
        try:
            self._users_storage.delete(session_id)
            return True
        except self._users_storage.errorSupplier.ConditionalCheckFailedException:
            return False

    def lock_node(self, path: str, timestamp: int) -> Tuple[bool, Optional[Node]]:

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
            # store raw provider data
            data = ret["Attributes"]
            n: Optional[Node] = None
            # node already exists
            if "cFxidSys" in data:
                n = Node(path)
                n.created = Version(
                    SystemCounter.from_provider_schema(data["cFxidSys"]),
                    EpochCounter.from_provider_schema(data["cFxidEpoch"]),
                )
                n.modified = Version(
                    SystemCounter.from_provider_schema(data["mFxidSys"]),
                    EpochCounter.from_provider_schema(data["mFxidEpoch"]),
                )
            return (True, n)
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return (False, None)

    def commit_node(self, node: Node, timestamp: int) -> bool:

        # FIXME: move this to the interface of control driver
        # we set the timelock value to the timestamp
        # for comparison, we subtract from the timestamp the maximum lock holding time
        try:
            # FIXME: proper version update
            self._state_storage._dynamodb.update_item(
                TableName=self._state_storage.storage_name,
                # path to the node
                Key={"path": {"S": node.path}},
                # create timelock
                UpdateExpression="REMOVE timelock SET mFxidSys= :modifiedStamp",
                # lock doesn't exist or it's already expired
                ConditionExpression="(attribute_exists(timelock)) "
                "and (timelock = :mytimelock)",
                # timelock value
                ExpressionAttributeValues={
                    ":mytimelock": {"N": str(timestamp)},
                    ":modifiedStamp": node.modified.system.version,
                },
                ReturnConsumedCapacity="TOTAL",
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
            return SystemCounter.from_provider_schema(ret["Attributes"]["cFxidSys"])
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return None
