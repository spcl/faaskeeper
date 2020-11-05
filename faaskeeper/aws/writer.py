import os
import socket
from typing import Dict, Callable

import boto3

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


def create_node(path: str, data: str, verbose_output: bool) -> dict:

    if verbose_output:
        print(f"Attempting to create node at {path}")


    return {}


def set_data():
    """
        The algorithm works as follows:
        1) Read node data
    """
    pass


def delete_node():
    pass


ops: Dict[str, Callable[[dict, bool], dict]] = {
    "create_node": create_node,
    "set_data": set_data,
    "delete_node": delete_node,
}


def handler(event: dict, context: dict):

    events = event["Records"]
    verbose_output = os.environ["VERBOSE_LOGGING"]
    print(event)
    processed_events = 0
    for record in events:
        if record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]

            """
                Handle malformed events correctly.
            """
            if any(k not in write_event.keys() for k in mandatory_event_fields):
                continue
            if write_event["op"] not in ops:
                continue

            ret = ops[write_event["op"]](write_event, verbose_output)
            processed_events += 1

    print(f"Successfully processed {processed_event} records out of {len(events)}")
