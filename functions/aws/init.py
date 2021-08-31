import boto3


def init(service_name: str, region: str):

    dynamodb = boto3.client("dynamodb", region_name=region)
    # clean state table
    dynamodb.put_item(
        TableName=f"{service_name}-state",
        Item={"path": {"S": "fxid"}, "data": {"L": [{"N": "0"}]}},
    )
