import boto3

import functions.aws.model as model
from faaskeeper.node import Node
from faaskeeper.version import SystemCounter, Version


def init(service_name: str, region: str):

    dynamodb = boto3.client("dynamodb", region_name=region)
    # clean state table
    dynamodb.put_item(
        TableName=f"{service_name}-state",
        Item={"path": {"S": "fxid"}, "cFxidSys": {"L": [{"N": "0"}]}},
    )

    # initialize root
    dynamodb.put_item(
        TableName=f"{service_name}-state",
        Item={
            "path": {"S": "/"},
            "cFxidSys": {"L": [{"N": "0"}]},
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "mFxidEpoch": {"NS": ["0"]},
            "children": {"L": []},
        },
    )
    dynamodb.put_item(
        TableName=f"{service_name}-data",
        Item={
            "path": {"S": "/"},
            "cFxidSys": {"L": [{"N": "0"}]},
            "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "mFxidEpoch": {"NS": ["0"]},
            "children": {"L": []},
        },
    )

    # Initialize root node for S3
    s3 = model.UserS3Storage(bucket_name=f"{service_name}-data")
    node = Node("/")
    node.created = Version(SystemCounter.from_raw_data([0]), None)
    node.modified = Version(SystemCounter.from_raw_data([0]), None)
    node.children = []
    node.data = b""
    s3.write(node)
