import base64
import json
import pathlib
import socket
from datetime import datetime
from time import sleep
from typing import Callable, Dict, Optional

from faaskeeper.node import Node, NodeDataType
from faaskeeper.version import Version
from functions.aws.config import Config

mandatory_event_fields = [
    "op",
    "path",
    "user",
    "version",
    "sourceIP",
    "sourcePort",
    "data",
]

config = Config.instance()


def verify_event(id: str, write_event: dict, verbose_output: bool, flags=[]) -> bool:
    """
        Handle malformed events correctly.
    """
    if any(k not in write_event.keys() for k in [*mandatory_event_fields, *flags]):
        if verbose_output:
            print(
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


def create_node(id: str, write_event: dict, verbose_output: bool) -> Optional[dict]:

    if not verify_event(id, write_event, verbose_output, ["flags"]):
        return None

    try:
        # TODO: ephemeral
        # TODO: sequential
        path = get_object(write_event["path"])
        if verbose_output:
            print(f"Attempting to create node at {path}")

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
        node.data = base64.b64decode(data)

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
        config.user_storage.write(node)
        config.user_storage.update(parent_node, set([NodeDataType.CHILDREN]))

        # FIXME: version
        return {
            "status": "success",
            "path": path,
            "system_counter": node.created.system.serialize(),
        }
    except Exception:
        # Report failure to the user
        print("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def deregister_session(
    id: str, write_event: dict, verbose_output: bool
) -> Optional[dict]:

    session_id = get_object(write_event["session_id"])
    try:
        # TODO: remove ephemeral nodes
        # FIXME: check return value
        if config.system_storage.delete_user(session_id):
            return {"status": "success", "session_id": session_id}
        else:
            if verbose_output:
                print(f"Attempting to remove non-existing user {session_id}")
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


def set_data(id: str, write_event: dict, verbose_output: bool) -> Optional[dict]:

    # FIXME: version
    # FIXME: full conditional update
    if not verify_event(id, write_event, verbose_output):
        return None
    try:
        path = get_object(write_event["path"])
        # version = get_object(write_event["version"])
        if verbose_output:
            print(f"Attempting to write data at {path}")

        # FIXME :limit number of attempts
        while True:
            timestamp = int(datetime.now().timestamp())
            lock, system_node = config.system_storage.lock_node(path, timestamp)
            if not lock:
                sleep(2)
            else:
                break

        # does the node exist?
        if system_node is None:
            config.system_storage.unlock_node(path, timestamp)
            return {"status": "failure", "path": path, "reason": "node_doesnt_exist"}

        counter = config.system_storage.increase_system_counter(WRITER_ID)
        if counter is None:
            return {"status": "failure", "reason": "unknown"}

        # FIXME: distributor
        # FIXME: epoch
        # store only the new data and the modified version counter

        data = get_object(write_event["data"])
        system_node.modified = Version(counter, None)
        system_node.data = base64.b64decode(data)

        if not config.system_storage.commit_node(system_node, timestamp):
            return {"status": "failure", "reason": "unknown"}

        """
            On DynamoDB we skip updating the created version as it doesn't change.
            On S3, we need to write this every single time.
        """
        config.user_storage.update(
            system_node, set([NodeDataType.MODIFIED, NodeDataType.DATA])
        )

        return {
            "status": "success",
            "path": path,
            "modified_system_counter": system_node.modified.system.serialize(),
        }
    except Exception:
        # Report failure to the user
        print("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def delete_node(id: str, write_event: dict, verbose_output: bool) -> Optional[dict]:
    return None


ops: Dict[str, Callable[[str, dict, bool], Optional[dict]]] = {
    "create_node": create_node,
    "set_data": set_data,
    "delete_node": delete_node,
    "deregister_session": deregister_session,
}


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

            op = get_object(write_event["op"])
            if op not in ops:
                if verbose_output:
                    print(
                        "Unknown operation {op} with ID {id}, "
                        "timestamp {timestamp}".format(
                            op=get_object(write_event["op"]),
                            id=record["eventID"],
                            timestamp=write_event["timestamp"],
                        )
                    )
                continue

            ret = ops[op](record["eventID"], write_event, verbose_output)
            if not ret:
                continue
            notify(write_event, ret)
            print(ret)
            processed_events += 1

    print(f"Successfully processed {processed_events} records out of {len(events)}")
