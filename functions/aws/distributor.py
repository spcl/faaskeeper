import hashlib
import json
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import boto3

from faaskeeper.watch import WatchEventType
from functions.aws.config import Config
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorDeleteNode,
    DistributorEvent,
    DistributorEventType,
    DistributorSetData,
)
from functions.aws.model.watches import Watches

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

# FIXME: configure
regions = ["us-east-1"]
region_clients = {}
region_watches = {}
epoch_counters: Dict[str, List[str]] = {}
for r in regions:
    region_clients[r] = boto3.client("lambda", region_name=r)
    region_watches[r] = Watches(config.deployment_name, r)
    epoch_counters[r] = []
executor = ThreadPoolExecutor(max_workers=2 * len(regions))


def get_object(obj: dict):
    return next(iter(obj.values()))


def launch_watcher(region: str, json_in: dict):
    """
    (1) Submit watcher
    (2) Wait for completion
    (3) Remove ephemeral counter.
    """
    ret = region_clients[region].invoke(
        FunctionName=f"{config.deployment_name}-watch",
        InvocationType="RequestResponse",
        Payload=json.dumps(json_in).encode(),
    )
    print(ret)


# def query_watch_id(region: str, node_path: str):
#    return region_watches[region].get_watch_counters(node_path)


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
    try:
        watches_submitters = []
        for record in events:
            if record["eventName"] != "INSERT":
                continue

            write_event = record["dynamodb"]["NewImage"]
            print(write_event)

            # FIXME: hide under abstraction, boto3 deserialize
            event_type = DistributorEventType(int(write_event["type"]["N"]))
            print(event_type)
            operation: DistributorEvent
            counters = []
            watches = {}
            if event_type == DistributorEventType.CREATE_NODE:
                operation = DistributorCreateNode.deserialize(write_event)
            elif event_type == DistributorEventType.SET_DATA:
                operation = DistributorSetData.deserialize(write_event)
                hashed_path = hashlib.md5(operation.node.path.encode()).hexdigest()
                counters.append(
                    f"{hashed_path}_{WatchEventType.NODE_DATA_CHANGED.value}"
                    f"_{operation.node.modified.system.sum}"
                )
                watches = {
                    "path": operation.node.path,
                    "event": WatchEventType.NODE_DATA_CHANGED.value,
                    "timestamp": operation.node.modified.system.sum,
                }
            elif event_type == DistributorEventType.DELETE_NODE:
                operation = DistributorDeleteNode.deserialize(write_event)
            else:
                raise NotImplementedError()
            try:
                # write new data
                ret = operation.execute(config.user_storage)
                print(ret, watches)
                # start watch delivery
                for r in regions:
                    if event_type == DistributorEventType.SET_DATA:
                        print("submit", watches)
                        watches_submitters.append(
                            executor.submit(launch_watcher, r, watches)
                        )
                for r in regions:
                    epoch_counters[r].extend(counters)
                if ret:
                    # notify client about success
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
                    write_event, {"status": "failure", "reason": "distributor failure"},
                )
        for f in watches_submitters:
            f.result()
    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()

    print(f"Successfully processed {processed_events} records out of {len(events)}")
