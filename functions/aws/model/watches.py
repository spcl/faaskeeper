from typing import List

from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.watch import WatchType
from functions.aws.control.dynamo import DynamoStorage as DynamoDriver

from collections import namedtuple

class Watches:
        
    Watch_Event = namedtuple("Watch_Event", ["watch_event_type","watch_type", "node_path", "mFxidSys"])

    def __init__(self, storage_name: str, region: str):
        self._storage = DynamoDriver(f"{storage_name}-watch", "path")
        self._region = region
        self._type_deserializer = TypeDeserializer()
        self._counters = {
            WatchType.GET_DATA: "getData",
            WatchType.EXISTS: "createNode",
            WatchType.GET_CHILDREN: "getChildrenID",
        }

    def query_watches(self, node_path: str, counters: List[WatchType]):
        try:

            ret = self._storage.read(node_path)

            data = []
            if "Item" in ret:
                for c in counters:
                    if self._counters.get(c) in ret["Item"]:
                        data.append(
                            (
                                c,
                                node_path,
                                self._type_deserializer.deserialize(
                                    ret["Item"][self._counters.get(c)]
                                ),
                            )
                        )
            return data
        except self._storage.errorSupplier.ResourceNotFoundException:
            return []

    def get_watches(self, node_path: str, counters: List[WatchType]):
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
                ReturnValues="UPDATED_OLD",
                ReturnConsumedCapacity="TOTAL",
            )
            # for c in self._counters:
            #    if c in ret["Item"]:
            #        counters.append(self._type_deserializer.deserialize(ret["Item"][c]))

            data = []
            if "Attributes" in ret:
                for c in counters:
                    data.append(
                        (
                            c,
                            self._type_deserializer.deserialize(
                                ret["Attributes"][self._counters.get(c)]  # type: ignore
                            ),
                        )
                    )
            return data
        except self._storage.errorSupplier.ResourceNotFoundException:
            return []
