from typing import Set, Union

import boto3
from boto3.dynamodb.types import TypeSerializer

from faaskeeper.node import Node, NodeDataType
from faaskeeper.stats import StorageStatistics

from .storage import Storage


class DynamoStorage(Storage):
    def __init__(self, table_name: str, key_name: str):
        # key_name corresponds to KeySchema in yaml
        super().__init__(table_name)
        self._dynamodb = boto3.client("dynamodb")
        self._type_serializer = TypeSerializer()
        self._key_name = key_name

    def write(self, key: str, data: Union[dict, bytes]):
        """DynamoDb write"""

        # Completely replace the existing data
        ret = self._dynamodb.put_item(
            TableName=self.storage_name,
            Item=data,  # type: ignore
            # ExpressionAttributeNames={"#P": self._key_name},
            # ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )
        # print("write", ret["ConsumedCapacity"])
        StorageStatistics.instance().add_write_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )
        return ret

    def update(self, key: str, data: dict):
        """DynamoDb update"""

        def get_object(obj: dict):
            return next(iter(obj.values()))

        ret = self._dynamodb.update_item(
            TableName=self.storage_name,
            Key={self._key_name: {"S": key}},
            ConditionExpression="attribute_exists(#P)",
            UpdateExpression="SET #D = :data ADD version :inc",
            ExpressionAttributeNames={"#D": "data", "#P": "path"},
            ExpressionAttributeValues={
                ":version": {"N": get_object(data["version"])},
                ":inc": {"N": "1"},
                # ":data": {"B": base64.b64decode(get_object(data["data"]))},
                ":data": {"B": get_object(data["data"])},
            },
            ReturnConsumedCapacity="TOTAL",
        )
        StorageStatistics.instance().add_write_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

    # def _toSchema(self, node: Node):
    #    # FIXME: pass epoch counter value
    #    schema = {
    #        "path": {"S": node.path},
    #        "data": {"B": node.data},
    #        "mFxidSys": node.modified.system.version,
    #        "mFxidEpoch": {"NS": ["0"]},
    #    }
    #    if node.created.system:
    #        schema = {
    #            **schema,
    #            "cFxidSys": node.created.system.version,
    #            "cFxidEpoch": {"NS": ["0"]},
    #        }
    #    return schema
    def _toSchema(self, node: Node):
        # FIXME: pass epoch counter value
        schema = {
            ":data": {"B": node.data_b64},
            ":mFxidSys": node.modified.system.version,
            ":mFxidEpoch": {"NS": ["0"]},
        }
        return schema

    # FIXME: merge with exisitng serialization code
    def update_node(self, node: Node, updates: Set[NodeDataType]):

        update_expr = "SET "
        schema: dict = {}
        attribute_names = {"#P": "path"}

        # FIXME: pass epoch counter value
        if NodeDataType.DATA in updates:
            # DynamoDB expects base64-encoded data
            # however, boto3 ALWAYS applies encoding
            # thus, we need to decode to let boto3 do
            # another layer of decoding
            #
            # https://github.com/aws/aws-cli/issues/1097
            schema[":data"] = {"B": node.data}
            update_expr = f"{update_expr} #D = :data,"
            attribute_names["#D"] = "data"
        if NodeDataType.CREATED in updates:
            schema = {
                **schema,
                ":cFxidSys": node.created.system.version,
                # ":cFxidEpoch": {"NS": ["0"]},
            }
            update_expr = f"{update_expr} cFxidSys = :cFxidSys,"
        if NodeDataType.MODIFIED in updates:
            schema = {
                **schema,
                ":mFxidSys": node.modified.system.version,
            }
            if node.modified.epoch:
                # FIXME: hide under abstraction
                assert node.modified.epoch.version is not None
                counters = list(node.modified.epoch.version)
                if len(counters) == 0:
                    counters = [""]
                schema = {**schema, ":mFxidEpoch": {"SS": counters}}
            else:
                schema = {**schema, ":mFxidEpoch": {"SS": [""]}}
            update_expr = (
                f"{update_expr} mFxidSys = :mFxidSys, mFxidEpoch = :mFxidEpoch,"
            )
        if NodeDataType.CHILDREN in updates:
            schema[":children"] = self._type_serializer.serialize(node.children)
            update_expr = f"{update_expr} children = :children,"
        # strip traling comma - boto3 will not accept that
        update_expr = update_expr[:-1]

        ret = self._dynamodb.update_item(
            TableName=self.storage_name,
            Key={self._key_name: {"S": node.path}},
            ConditionExpression="attribute_exists(#P)",
            UpdateExpression=update_expr,
            ExpressionAttributeNames=attribute_names,
            ExpressionAttributeValues=schema,
            ReturnConsumedCapacity="TOTAL",
        )
        StorageStatistics.instance().add_write_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

    def read(self, key: str):
        """DynamoDb read"""

        ret = self._dynamodb.get_item(
            TableName=self.storage_name,
            Key={self._key_name: {"S": key}},
            ReturnConsumedCapacity="TOTAL",
            ConsistentRead=True,
        )
        StorageStatistics.instance().add_read_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

        return ret

    def delete(self, key: str):
        """DynamoDb delete"""

        ret = self._dynamodb.delete_item(
            TableName=self.storage_name,
            Key={self._key_name: {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )
        StorageStatistics.instance().add_write_units(
            ret["ConsumedCapacity"]["CapacityUnits"]
        )

    @property
    def errorSupplier(self):
        """DynamoDb exceptions"""

        return self._dynamodb.exceptions
