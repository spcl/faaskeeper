import json
import logging
import pathlib
import time
import base64
from datetime import datetime
from time import sleep
from typing import Callable, Dict, List, Optional

from faaskeeper.node import Node, NodeDataType
from faaskeeper.stats import StorageStatistics
from faaskeeper.version import Version
from functions.aws.config import Config
from functions.aws.control.distributor_events import (
    DistributorCreateNode,
    DistributorDeleteNode,
    DistributorSetData,
)

mandatory_event_fields = [
    "op",
    "path",
    "session_id",
    "version",
    "sourceIP",
    "sourcePort",
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


def create_node(id: str, write_event: dict) -> Optional[dict]:

    if not verify_event(id, write_event, ["flags"]):
        return {"status": "failure", "reason": "incorrect_request"}

    try:
        # TODO: ephemeral
        # TODO: sequential
        path = get_object(write_event["path"])
        logging.info(f"Attempting to create node at {path}")

        data = get_object(write_event["data"])

        # FIXME :limit number of attempts
        while True:
            timestamp = int(datetime.now().timestamp())
            lock, node = config.system_storage.lock_node(path, timestamp)
            if not lock:
                sleep(2)
            else:
                break

        # does the node exist?
        if node is not None:
            config.system_storage.unlock_node(path, timestamp)
            return {"status": "failure", "path": path, "reason": "node_exists"}

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
        # does the node does not exist?
        if parent_node is None:
            config.system_storage.unlock_node(str(parent_path), parent_timestamp)
            config.system_storage.unlock_node(path, timestamp)
            return {
                "status": "failure",
                "path": str(parent_path),
                "reason": "node_doesnt_exist",
            }

        counter = config.system_storage.increase_system_counter(WRITER_ID)
        if counter is None:
            return {"status": "failure", "reason": "unknown"}

        # FIXME: epoch
        # store the created and the modified version counter
        node = Node(path)
        node.created = Version(counter, None)
        node.modified = Version(counter, None)
        node.children = []
        # we propagate data to another queue, we should use the already
        # base64-encoded data
        node.data_b64 = data

        # FIXME: make both operations concurrently
        # unlock parent
        # parent now has one child more
        parent_node.children.append(pathlib.Path(path).name)
        config.system_storage.commit_node(
            parent_node, parent_timestamp, set([NodeDataType.CHILDREN])
        )
        # commit node
        config.system_storage.commit_node(
            node,
            timestamp,
            set([NodeDataType.CREATED, NodeDataType.MODIFIED, NodeDataType.CHILDREN]),
        )
        # FIXME: distributor - make sure both have the same version
        # config.user_storage.write(node)
        # config.user_storage.update(parent_node, set([NodeDataType.CHILDREN]))

        assert config.distributor_queue
        config.distributor_queue.push(
            write_event["timestamp"],
            write_event["sourceIP"],
            write_event["sourcePort"],
            counter,
            DistributorCreateNode(
                get_object(write_event["session_id"]), node, parent_node
            ),
        )

        return None
        # FIXME: version
        # return {
        #    "status": "success",
        #    "path": path,
        #    "system_counter": node.created.system.serialize(),
        # }
    except Exception:
        # Report failure to the user
        logging.error("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def deregister_session(id: str, write_event: dict) -> Optional[dict]:

    session_id = get_object(write_event["session_id"])
    try:
        # TODO: remove ephemeral nodes
        # FIXME: check return value
        if config.system_storage.delete_user(session_id):
            return {"status": "success", "session_id": session_id}
        else:
            logging.error(f"Attempting to remove non-existing user {session_id}")
            return {
                "status": "failure",
                "session_id": session_id,
                "reason": "session_does_not_exist",
            }
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}


def set_data(id: str, write_event: dict) -> Optional[dict]:

    begin = time.time()
    # FIXME: version
    # FIXME: full conditional update
    if not verify_event(id, write_event):
        return None
        return {"status": "failure", "reason": "incorrect_request"}
    try:
        path = get_object(write_event["path"])
        # version = get_object(write_event["version"])
        logging.info(f"Attempting to write data at {path}")

        begin_lock = time.time()
        # FIXME :limit number of attempts
        while True:
            timestamp = int(datetime.now().timestamp())
            lock, system_node = config.system_storage.lock_node(path, timestamp)
            if not lock:
                sleep(2)
            else:
                break
        end_lock = time.time()
        logging.info(f"Acquired lock at {path}")

        # does the node exist?
        if system_node is None:
            config.system_storage.unlock_node(path, timestamp)
            return {"status": "failure", "path": path, "reason": "node_doesnt_exist"}

        begin_atomic = time.time()
        counter = config.system_storage.increase_system_counter(WRITER_ID)
        if counter is None:
            return {"status": "failure", "reason": "unknown"}
        end_atomic = time.time()
        logging.info(f"Incremented system counter")

        # FIXME: distributor
        # FIXME: epoch
        # store only the new data and the modified version counter

        data = get_object(write_event["data"])
        system_node.modified = Version(counter, None)
        system_node.data_b64 = data
        logging.info(f"Finished commit preparation")

        begin_commit = time.time()
        if not config.system_storage.commit_node(system_node, timestamp):
            return {"status": "failure", "reason": "unknown"}
        end_commit = time.time()
        logging.info(f"Finished commit")

        begin_push = time.time()
        assert config.distributor_queue
        config.distributor_queue.push(
            write_event["timestamp"],
            write_event["sourceIP"],
            write_event["sourcePort"],
            counter,
            DistributorSetData(get_object(write_event["session_id"]), system_node),
        )
        end_push = time.time()
        logging.info(f"Finished pushing update")

        end = time.time()

        global repetitions
        global sum_total
        global sum_lock
        global sum_atomic
        global sum_commit
        global sum_push
        repetitions += 1
        sum_total += end - begin
        sum_lock += end_lock - begin_lock
        sum_atomic += end_atomic - begin_atomic
        sum_commit += end_commit - begin_commit
        sum_push += end_push - begin_push
        if repetitions % 100 == 0:
            print("RESULT_TOTAL", sum_total)
            print("RESULT_LOCK", sum_lock)
            print("RESULT_ATOMIC", sum_atomic)
            print("RESULT_COMMIT", sum_commit)
            print("RESULT_PUSH", sum_push)

        return None

    except Exception:
        # Report failure to the user
        print("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def delete_node(id: str, write_event: dict) -> Optional[dict]:

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
            write_event["timestamp"],
            write_event["sourceIP"],
            write_event["sourcePort"],
            counter,
            DistributorDeleteNode(
                get_object(write_event["sesion_id"]), node, parent_node
            ),
        )
        return None
    except Exception:
        # Report failure to the user
        print("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


ops: Dict[str, Callable[[str, dict], Optional[dict]]] = {
    "create_node": create_node,
    "set_data": set_data,
    "delete_node": delete_node,
    "deregister_session": deregister_session,
}


# def get_object(obj: dict):
#    return next(iter(obj.values()))
def get_object(obj: dict):
    return next(iter(obj.values()))


# def notify(write_event: dict, ret: dict):
#
#    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#        try:
#            s.settimeout(2)
#            source_ip = get_object(write_event["sourceIP"])
#            source_port = int(get_object(write_event["sourcePort"]))
#            # print(source_ip, source_port)
#            s.connect((source_ip, source_port))
#            print(f"Connected to {source_ip}:{source_port}")
#            s.sendall(
#                json.dumps(
#                    {**ret, "event": get_object(write_event["timestamp"])}
#                ).encode()
#            )
#        except socket.timeout:
#            print(f"Notification of client {source_ip}:{source_port} failed!")


def handler(event: dict, context):

    events = event["Records"]
    logging.info(f"Begin processing {len(events)} events")
    processed_events = 0
    StorageStatistics.instance().reset()
    for record in events:
        if "dynamodb" in record and record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]
            event_id = record["eventID"]

            # when launching from a trigger, the binary vlaue is not automatically base64 decoded
            # however, we can't put base64 encoded data to boto3 - it ALWAYS applies encoding,
            # regardless of the format of data
            # https://github.com/boto/boto3/issues/3291
            # https://github.com/aws/aws-cli/issues/1097
            if "data" in write_event:
                write_event["data"]["B"] = base64.b64decode(write_event["data"]["B"])

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

        op = get_object(write_event["op"])
        if op not in ops:
            logging.error(
                "Unknown operation {op} with ID {id}, "
                "timestamp {timestamp}".format(
                    op=get_object(write_event["op"]),
                    id=event_id,
                    timestamp=write_event["timestamp"],
                )
            )
            continue

        ret = ops[op](event_id, write_event)
        if ret:
            if ret["status"] == "failure":
                logging.error(f"Failed processing write event {event_id}: {ret}")
            # Failure - notify client
            config.client_channel.notify(
                get_object(write_event["session_id"]),
                get_object(write_event["timestamp"]),
                write_event,
                ret,
            )
            # notify(write_event, ret)
            continue
        else:
            processed_events += 1

    # print(f"Successfully processed {processed_events} records out of {len(events)}")
    print(
        f"Request: {context.aws_request_id} "
        f"Read: {StorageStatistics.instance().read_units}\t"
        f"Write: {StorageStatistics.instance().write_units}"
    )
