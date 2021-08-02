import boto3


class Dynamo(Storage):
    def __init__(self):
        self.dynamodb = boto3.client("dynamodb")

    def write(self, storage_name: str, key: str, data: str):
        """S3/DynamoDB write"""

        self.dynamodb.put_item(
            TableName=f"{storage_name}-data",
            Item={
                "path": {"S": key},
                "data": {"B": data},
                "dFxid": {"N": "0"},
                "cFxid": {"N": "0"},
                "mFxid": {"N": "0"},
                "ephemeralOwner": {"S": ""},
            },
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )

    def read(self, storage_name: str, key: str):
        """S3/DynamoDB read"""

        return self.client.get_item(
            TableName=storage_name, Key={'path': {'S': key}})

    def delete(self, storage_name: str, key: str):
        """S3/DynamoDB delete"""

        self.dynamodb.delete_item(
            TableName=f"{storage_name}-state",
            Key={"type": {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )
