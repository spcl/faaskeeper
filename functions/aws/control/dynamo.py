import boto3
from functions.aws.control.storage import Storage


class DynamoStorage(Storage):
    def __init__(self):
        self.dynamodb = boto3.client("dynamodb")

    def write(self, storage_name: str, key: str, data: str):
        """DynamoDb write"""

        dynamodb.put_item(
            TableName=f"{storage_name}-data",
            Item=Storage._toSchema(key, data),
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )

    def read(self, storage_name: str, key: str):
        """DynamoDb read"""

        return dynamodb.get_item(
            TableName=storage_name,
            Key={'path': {'S': key}}
        )

    def delete(self, storage_name: str, key: str):
        """DynamoDb delete"""

        dynamodb.delete_item(
            TableName=f"{storage_name}-state",
            Key={"type": {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )

    def errorSupplier(self):
        """DynamoDb exceptions"""

        return dynamodb.exceptions
