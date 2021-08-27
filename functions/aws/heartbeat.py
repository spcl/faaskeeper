import os
from datetime import datetime

import boto3

# from typing import Callable, Dict, Optional


mandatory_event_fields = [
    "op",
    "path",
    "user",
    "version",
    "flags",
    "sourceIP",
    "sourcePort",
    "data",
]
dynamodb = boto3.client("dynamodb")
table_name = os.environ["DYNAMODB_TABLE"]


def handler(event: dict, context: dict):

    print(handler)

    print(f"{str(datetime.now())} Called heartbeat")
    try:
        ret = dynamodb.scan(
            TableName=f"{table_name}-state",
            ConsistentRead=True,
            ReturnConsumedCapacity="TOTAL",
        )
        print(ret)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    handler({}, {})
