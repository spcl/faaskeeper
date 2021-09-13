from typing import List

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.watch import WatchType
from functions.aws.control.dynamo import DynamoStorage as DynamoDriver


class Watches:
    def __init__(self, storage_name: str, region: str):
        self._storage = DynamoDriver(f"{storage_name}-watch", "path")
        self._region = region
        self._type_deserializer = TypeDeserializer()
        self._counters = {
            WatchType.GET_DATA: "getData",
            WatchType.CREATE_NODE: "createNode",
            WatchType.GET_CHILDREN: "getChildrenID",
        }

    def get_watches(self, node_path: str, counters: List[WatchEventType]) -> List[int]:
        try:
            # we always commit the modified stamp
            update_expr = "REMOVE "
            for c in counters:
                update_expr = f"{update_expr} {self._counters.get(c)},"
            update_expr = update_expr[:-1]

            ret = self._storage._dynamodb.update_item(
                TableName=self._storage.storage_name,
                # path to the node
                Key={"path": {"S": node_path}},
                # create timelock
                UpdateExpression=update_expr,
                ReturnValues="ALL_OLD",
                ReturnConsumedCapacity="TOTAL",
            )
            # for c in self._counters:
            #    if c in ret["Item"]:
            #        counters.append(self._type_deserializer.deserialize(ret["Item"][c]))

            return ret["Item"]
        except self._storage.errorSupplier.ResourceNotFoundException:
            return []
