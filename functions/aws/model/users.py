from boto3.dynamodb.types import TypeDeserializer

from functions.aws.control.dynamo import DynamoStorage as DynamoDriver


class Users:
    def __init__(self, storage_name: str, region: str):
        # FIXME: regionalize
        self._storage = DynamoDriver(f"{storage_name}-users", "user")
        self._region = region
        self._type_deserializer = TypeDeserializer()

    def get_users(self):
        response = self._storage._dynamodb.scan(
            TableName=self._storage.storage_name, ReturnConsumedCapacity="TOTAL"
        )
        items = response["Items"]
        read_capacity = response["ConsumedCapacity"]["CapacityUnits"]
        # FIXME: parallelize it with submission of messages
        while "LastEvaluatedKey" in response:
            print(response["LastEvaluatedKey"])
            response = self._storage._dynamodb.scan(
                TableName=self._storage.storage_name,
                ReturnConsumedCapacity="TOTAL",
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response["Items"])
            read_capacity += response["ConsumedCapacity"]["CapacityUnits"]
        return items, read_capacity
