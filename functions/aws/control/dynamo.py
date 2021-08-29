import base64
from typing import Union

import boto3

from .storage import Storage


class DynamoStorage(Storage):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self._dynamodb = boto3.client("dynamodb")

    def write(self, key: str, data: Union[bytes, str]):
        """DynamoDb write"""

        return self._dynamodb.put_item(
            TableName=self.storage_name,
            Item=data,
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )

    def update(self, key: str, data: dict):
        """DynamoDb update"""

        def get_object(obj: dict):
            return next(iter(obj.values()))

        self._dynamodb.update_item(
            TableName=self.storage_name,
            Key={"path": {"S": key}},
            ConditionExpression="(attribute_exists(#P)) and (version = :version)",
            UpdateExpression="SET #D = :data ADD version :inc",
            ExpressionAttributeNames={"#D": "data", "#P": "path"},
            ExpressionAttributeValues={
                ":version": {"N": get_object(data["version"])},
                ":inc": {"N": "1"},
                ":data": {"B": base64.b64decode(get_object(data["data"]))},
            },
            ReturnConsumedCapacity="TOTAL",
        )

    def read(self, key: str):
        """DynamoDb read"""

        return self._dynamodb.get_item(
            TableName=self.storage_name, Key={"path": {"S": key}}
        )

    def delete(self, key: str):
        """DynamoDb delete"""

        self._dynamodb.delete_item(
            TableName=self.storage_name,
            Key={"type": {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )

    @property
    def errorSupplier(self):
        """DynamoDb exceptions"""

        return self._dynamodb.exceptions
