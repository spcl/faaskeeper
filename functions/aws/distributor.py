import json
import socket

from functions.aws.config import Config
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorDeleteNode,
    DistributorEvent,
    DistributorEventType,
    DistributorSetData,
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
            print(write_event)
            source_ip = get_object(write_event["sourceIP"])
            source_port = int(get_object(write_event["sourcePort"]))
            s.connect((source_ip, source_port))
            s.sendall(
                json.dumps(
                    {**ret, "event": get_object(write_event["user_timestamp"])}
                ).encode()
            )
        except socket.timeout:
            print(f"Notification of client {source_ip}:{source_port} failed!")


def handler(event: dict, context: dict):

    events = event["Records"]
    verbose_output = config.verbose
    if verbose_output:
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
            elif event_type == DistributorEventType.SET_DATA:
                operation = DistributorSetData.deserialize(write_event)
            elif event_type == DistributorEventType.DELETE_NODE:
                operation = DistributorDeleteNode.deserialize(write_event)
            else:
                raise NotImplementedError()
            try:
                ret = operation.execute(config.user_storage)
                if ret:
                    notify(write_event, ret)
                    processed_events += 1
                else:
                    notify(
                        write_event,
                        {"status": "failure", "reason": "distributor failured"},
                    )
            except Exception:
                print("Failure!")
                import traceback

                traceback.print_exc()
                notify(
                    write_event, {"status": "failure", "reason": "distributor failure"}
                )

    print(f"Successfully processed {processed_events} records out of {len(events)}")
