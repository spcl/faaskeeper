from abc import ABC, abstractmethod
from typing import Tuple

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
    def lock_node(self, path: str, timestamp: int):
        pass

    @abstractmethod
    def commit_node(self, path: str, timestamp: int):
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

    def lock_node(self, path: str, timestamp: int) -> Tuple[bool, dict]:

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
            return (True, ret["Attributes"])
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return (False, {})

    def commit_node(self, path: str, timestamp: int) -> bool:

        # FIXME: move this to the interface of control driver
        # we set the timelock value to the timestamp
        # for comparison, we subtract from the timestamp the maximum lock holding time
        try:
            # FIXME: proper version update
            self._state_storage._dynamodb.update_item(
                TableName=self._state_storage.storage_name,
                # path to the node
                Key={"path": {"S": path}},
                # create timelock
                UpdateExpression="REMOVE timelock SET mFxidSys= :modifiedStamp",
                # lock doesn't exist or it's already expired
                ConditionExpression="(attribute_exists(timelock)) "
                "and (timelock = :mytimelock)",
                # timelock value
                ExpressionAttributeValues={
                    ":mytimelock": {"N": str(timestamp)},
                    ":modifiedStamp": {"L": [{"N": "0"}]},
                },
                ReturnConsumedCapacity="TOTAL",
            )
            return True
        except self._state_storage.errorSupplier.ConditionalCheckFailedException:
            return False
