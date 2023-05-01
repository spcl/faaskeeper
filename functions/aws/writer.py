import json
import logging
import pathlib
import time
from datetime import datetime
from time import sleep
from typing import Callable, Dict, List, Optional

from faaskeeper.node import Node, NodeDataType
from faaskeeper.stats import StorageStatistics
from faaskeeper.version import Version
from functions.aws.config import Config
from functions.aws.control.channel import Client
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorDeleteNode,
    DistributorSetData,
)
from functions.aws.operations import Executor
from functions.aws.operations import builder as operations_builder

mandatory_event_fields = [
    "op",
    "path",
    "session_id",
    "version",
    "data",
]

config = Config.instance()

repetitions = 0
sum_total = 0.0
sum_lock = 0.0
sum_atomic = 0.0
sum_commit = 0.0
sum_push = 0.0


def verify_event(id: str, write_event: dict, flags: List[str] = None) -> bool:

    events = []
    if flags is not None:
        events = [*mandatory_event_fields, *flags]
    else:
        events = [*mandatory_event_fields]

    """
        Handle malformed events correctly.
    """
    if any(k not in write_event.keys() for k in events):
        logging.error(
            "Incorrect event with ID {id}, timestamp {timestamp}".format(
                id=id, timestamp=write_event["timestamp"]
            )
        )
        return False
    return True


# FIXME: proper config
WRITER_ID = 0

"""
    The function has the following responsibilities:
    1) Create new node, returning success or failure if the node exists
    2) Set-up ACL permission on the node.
    3) Add the list to user's nodelist in case of an ephemeral node.
    4) Create sequential node by appending newest version.
    5) Create parents nodes to make sure the entire path exists.
    6) Look-up watches in a seperate table.
"""


def execute_operation(op_exec: Executor, client: Client) -> Optional[dict]:

    try:

        status, ret = op_exec.lock_and_read(config.system_storage)
        if not status:
            return ret

        # FIXME: revrse the order here
        status, ret = op_exec.commit_and_unlock(config.system_storage)
        if not status:
            return ret

        assert config.distributor_queue
        op_exec.distributor_push(client, config.distributor_queue)

        return ret

    except Exception:
        # Report failure to the user
        logging.error("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def delete_node(client: Client, id: str, write_event: dict) -> Optional[dict]:

    # if not verify_event(id, write_event, verbose_output, ["flags"]):
    #    return None

    try:
        # TODO: ephemeral
        # TODO: sequential
        path = get_object(write_event["path"])
        logging.info(f"Attempting to create node at {path}")

        # FIXME :limit number of attempts
        while True:
            timestamp = int(datetime.now().timestamp())
            lock, node = config.system_storage.lock_node(path, timestamp)
            if not lock:
                sleep(2)
            else:
                break

        # does the node not exist?
        if node is None:
            config.system_storage.unlock_node(path, timestamp)
            return {"status": "failure", "path": path, "reason": "node_doesnt_exist"}

        if len(node.children):
            config.system_storage.unlock_node(path, timestamp)
            return {"status": "failure", "path": path, "reason": "not_empty"}

        # lock the parent - unless we're already at the root
        node_path = pathlib.Path(path)
        parent_path = node_path.parent.absolute()
        parent_timestamp: Optional[int] = None
        while True:
            parent_timestamp = int(datetime.now().timestamp())
            parent_lock, parent_node = config.system_storage.lock_node(
                str(parent_path), parent_timestamp
            )
            if not lock:
                sleep(2)
            else:
                break
        assert parent_node

        counter = config.system_storage.increase_system_counter(WRITER_ID)
        if counter is None:
            return {"status": "failure", "reason": "unknown"}

        # remove child from parent node
        parent_node.children.remove(pathlib.Path(path).name)

        # commit system storage
        config.system_storage.commit_node(
            parent_node, parent_timestamp, set([NodeDataType.CHILDREN])
        )
        config.system_storage.delete_node(node, timestamp)

        assert config.distributor_queue
        config.distributor_queue.push(
            counter, DistributorDeleteNode(client.session_id, node, parent_node), client
        )
        return None
    except Exception:
        # Report failure to the user
        print("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


ops: Dict[str, Callable[[Client, str, dict], Optional[dict]]] = {
    # "create_node": create_node,
    # "set_data": set_data,
    "delete_node": delete_node,
    # "deregister_session": deregister_session,
}


# def get_object(obj: dict):
#    return next(iter(obj.values()))
def get_object(obj: dict):
    return next(iter(obj.values()))


def handler(event: dict, context):

    events = event["Records"]
    logging.info(f"Begin processing {len(events)} events")
    processed_events = 0
    StorageStatistics.instance().reset()
    for record in events:

        # FIXME: abstract away, hide the DynamoDB conversion
        if "dynamodb" in record and record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]
            event_id = record["eventID"]

        elif "body" in record:
            write_event = json.loads(record["body"])
            if "data" in record["messageAttributes"]:
                write_event["data"] = {
                    "B": record["messageAttributes"]["data"]["binaryValue"]
                }
            event_id = record["attributes"]["MessageDeduplicationId"]
            write_event["timestamp"] = {"S": event_id}
        else:
            raise NotImplementedError()

        client = Client.deserialize(write_event)

        # FIXME: hide DynamoDB serialization somewhere else
        parsed_event = {x: get_object(y) for x, y in write_event.items()}
        op = parsed_event["op"]

        executor, error = operations_builder(op, event_id, parsed_event)
        if executor is None:
            config.client_channel.notify(client, error)
            continue

        ret = execute_operation(executor, client)

        if ret:
            if ret["status"] == "failure":
                logging.error(f"Failed processing write event {event_id}: {ret}")
            else:
                processed_events += 1
            config.client_channel.notify(client, ret)
            continue
        else:
            processed_events += 1

    print(f"Successfully processed {processed_events} records out of {len(events)}")
    print(
        f"Request: {context.aws_request_id} "
        f"Read: {StorageStatistics.instance().read_units}\t"
        f"Write: {StorageStatistics.instance().write_units}"
    )
