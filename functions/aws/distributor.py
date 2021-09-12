import base64
import json
import socket
from datetime import datetime
from time import sleep
from typing import Callable, Dict, Optional

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import Version
from functions.aws.config import Config
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorEvent,
    DistributorEventType,
)

mandatory_event_fields = [
    "op" "path",
    "user",
    "version",
    "sourceIP",
    "sourcePort",
    "data",
]

config = Config.instance(False)

"""
    The data received from the queue includes:
    - client IP and port
    - updates

    We support the following cases:
    - create_node - writing user node (no data) and writing children in parent node
    - set_data - update counter and data
    - delete_node - delete node and overwrite parent nodes
"""


def process_update(write_event: dict, verbose_output: bool):
    pass


def get_object(obj: dict):
    return next(iter(obj.values()))


def notify(write_event: dict, ret: dict):

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.settimeout(2)
            source_ip = get_object(write_event["sourceIP"])
            source_port = int(get_object(write_event["sourcePort"]))
            s.connect((source_ip, source_port))
            s.sendall(
                json.dumps(
                    {**ret, "event": get_object(write_event["timestamp"])}
                ).encode()
            )
        except socket.timeout:
            print(f"Notification of client {source_ip}:{source_port} failed!")


def handler(event: dict, context: dict):

    events = event["Records"]
    verbose_output = config.verbose
    print(event)
    processed_events = 0
    for record in events:
        if record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]
            print(write_event)

            # FIXME: hide under abstraction, boto3 deserialize
            event_type = DistributorEventType(int(write_event["type"]["N"]))
            operation: DistributorEvent
            if event_type == DistributorEventType.CREATE_NODE:
                operation = DistributorCreateNode.deserialize(write_event)
            else:
                raise NotImplementedError()
            operation.execute(config.user_storage)
            # op = get_object(write_event["op"])
            # if op not in ops:
            #    if verbose_output:
            #        print(
            #            "Unknown operation {op} with ID {id}, "
            #            "timestamp {timestamp}".format(
            #                op=get_object(write_event["op"]),
            #                id=record["eventID"],
            #                timestamp=write_event["timestamp"],
            #            )
            #        )
            #    continue

            # ret = ops[op](record["eventID"], write_event, verbose_output)
            # if not ret:
            #    continue
            # notify(write_event, ret)
            # print(ret)
            processed_events += 1

    print(f"Successfully processed {processed_events} records out of {len(events)}")
