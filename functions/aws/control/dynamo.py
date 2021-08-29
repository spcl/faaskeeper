import base64

import boto3

from functions.aws.control.storage import Storage


class DynamoStorage(Storage):
    def __init__(self):
        self._dynamodb = boto3.client("dynamodb")

    def write(self, storage_name: str, key: str, data: str):
        """DynamoDb write"""

        self._dynamodb.put_item(
            TableName=f"{storage_name}-data",
            Item=Storage._toSchema(key, data),
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )

    def update(self, storage_name: str, key: str, data: dict):
        """DynamoDb update"""

        def get_object(obj: dict):
            return next(iter(obj.values()))

        self._dynamodb.update_item(
            TableName=f"{storage_name}-data",
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

    def read(self, storage_name: str, key: str):
        """DynamoDb read"""

        return self._dynamodb.get_item(TableName=storage_name, Key={"path": {"S": key}})

    def delete(self, storage_name: str, key: str):
        """DynamoDb delete"""

        self._dynamodb.delete_item(
            TableName=f"{storage_name}-state",
            Key={"type": {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )

    def errorSupplier(self):
        """DynamoDb exceptions"""

        return self._dynamodb.exceptions
