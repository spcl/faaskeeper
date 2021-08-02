import boto3
import base64
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

    def update(self, storage_name: str, key: str, data: dict):
        """DynamoDb update"""

        def get_object(obj: dict):
            return next(iter(obj.values()))

        dynamodb.update_item(
            TableName=f"{storage_name}-data",
            Key={
                "path": {"S": key},
                # "version": {"N": version},
            },
            # AttributeUpdates={
            #    "data": {
            #        "Value": {
            #            "B": base64.b64decode(get_object(write_event["data"]))
            #        }, #        "Action": { "PUT" }
            #    }
            # },
            # ExpressionAttributeNames={"#P": "path"},
            # ConditionExpression="(attribute_not_exists(#P)) and (version = :version)",
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
