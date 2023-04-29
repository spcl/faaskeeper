import boto3
from dotenv import dotenv_values

import functions.aws.model as model
from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version


def clean(service_name: str, region: str):

    envs = dotenv_values()
    s3_data_bucket = envs["S3_DATA_BUCKET"]
    s3_bucket = boto3.resource("s3").Bucket(s3_data_bucket)
    try:
        s3_bucket.objects.all().delete()
    except Exception:
        pass


def config(config_json: dict):

    envs = dotenv_values()
    s3_data_bucket = envs["S3_DATA_BUCKET"]
    config_json["aws"] = {"data-bucket": s3_data_bucket}

    return config_json


def init(service_name: str, region: str):

    envs = dotenv_values()
    s3_data_bucket = envs["S3_DATA_BUCKET"]
    assert s3_data_bucket is not None

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
            # "cFxidEpoch": {"SS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "pFxidSys": {"L": [{"N": "0"}]},
            # "mFxidEpoch": {"NS": ["0"]},
            "children": {"L": []},
        },
    )
    dynamodb.put_item(
        TableName=f"{service_name}-data",
        Item={
            "path": {"S": "/"},
            "cFxidSys": {"L": [{"N": "0"}]},
            # "cFxidEpoch": {"NS": ["0"]},
            "mFxidSys": {"L": [{"N": "0"}]},
            "mFxidEpoch": {"SS": [""]},
            "children": {"L": []},
        },
    )

    # Initialize root node for S3
    s3 = model.UserS3Storage(bucket_name=s3_data_bucket)
    node = Node("/")
    node.created = Version(SystemCounter.from_raw_data([0]), None)
    node.modified = Version(
        SystemCounter.from_raw_data([0]), EpochCounter.from_raw_data(set())
    )
    node.children = []
    node.data = b""
    s3.write(node)

    # initialize ephemeral counter
    # FIXME: do it for every region
    # dynamodb.put_item(
    #    TableName=f"{service_name}-watch",
    #    Item={"path": {"S": "epoch-counter"}, "counter": {"N": "0"}},
    # )
