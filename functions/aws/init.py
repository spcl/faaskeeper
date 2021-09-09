import boto3


def init(service_name: str, region: str):

    dynamodb = boto3.client("dynamodb", region_name=region)
    # clean state table
    dynamodb.put_item(
        TableName=f"{service_name}-state",
        Item={"path": {"S": "fxid"}, "cFxidSys": {"L": [{"N": "0"}]}},
    )
    # initialize root
    dynamodb.put_item(
        TableName=f"{service_name}-data",
        Item={
            "path": {"S": "/"},
            "cFxidSys": {"L": [{"N": "0"}]},
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "mFxidEpoch": {"NS": ["0"]},
        },
    )
