import boto3

dynamodb = boto3.client("dynamodb")

class Dynamo(Storage):
    def write(self, path: str, table_name: str, data: str) -> str:
        """S3/DynamoDB write"""
        dynamodb.put_item(
            TableName=f"{table_name}-data",
            Item={
                "path": {"S": path},
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

    def read(self, full_file_name: str) -> str:
        """S3/DynamoDB read"""
        pass

    def delete(self, table_name: str, key: str) -> str:
        """S3/DynamoDB delete"""
        dynamodb.delete_item(
            TableName=f"{table_name}-state",
            Key={"type": {"S": key}},
            ReturnConsumedCapacity="TOTAL",
        )