import base64
import json
import os
import socket
from typing import Dict, Callable, Optional

import boto3

mandatory_event_fields = [
    "op",
    "path",
    "user",
    "version",
    "sourceIP",
    "sourcePort",
    "data",
]
dynamodb = boto3.client("dynamodb")

def verify_event(id: str, write_event:dict, verbose_output: bool, flags = []) -> bool:
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

"""
    The function has the following responsibilities:
    1) Create new node, returning success or failure if the node exists
    2) Set-up ACL permission on the node.
    3) Add the list to user's nodelist in case of an ephemeral node.
    4) Create sequential node by appending newest version.
    5) Create parents nodes to make sure the entire path exists.
    6) Look-up watches in a seperate table.
"""

def create_node(id: str, write_event: dict, table_name: str, verbose_output: bool) -> Optional[dict]:

    if not verify_event(id, write_event, verbose_output, ["flags"]):
        return None

    try:
        # TODO: ephemeral
        # TODO: sequential
        # TODO: makepath
        path = get_object(write_event["path"])
        if verbose_output:
            print(f"Attempting to create node at {path}")
        """
            Path is a reserved keyword in AWS DynamoDB - we must rename.
        """
        ret = dynamodb.put_item(
            TableName=f"{table_name}-data",
            Item={
                "path": {"S": path},
                "version": {"N": "0"},
                "data": {"B": base64.b64decode(get_object(write_event["data"]))},
            },
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )
        print(get_object(write_event["data"]))
        print(ret)
        return {"status": "success", "path": path, "version": 0}
    except dynamodb.exceptions.ConditionalCheckFailedException:
        return {"status": "failure", "path": path, "reason": "node_exists"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}

def deregister_session(id: str, write_event: dict, table_name: str, verbose_output: bool) -> dict:

    session_id = get_object(write_event["session_id"])
    try:
        # TODO: remove ephemeral nodes
        ret = dynamodb.delete_item(
            TableName=f"{table_name}-state",
            Key={
                "type": {"S": session_id}
            },
            ReturnConsumedCapacity="TOTAL",
        )
        return {"status": "success", "session_id": session_id}
    except dynamodb.exceptions.ResourceNotFoundException:
        if verbose_output:
            print(f"Attempting to remove non-existing user {session_id}")
        return {"status": "failure", "session_id": session_id, "reason": "session_does_not_exist"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}

def set_data(id: str, write_event: dict, table_name: str, verbose_output: bool):

    print(write_event)
    if not verify_event(id, write_event, verbose_output):
        return None
    # FIXME: version
    try:
        path = get_object(write_event["path"])
        if verbose_output:
            print(f"Attempting to write data at {path}")
        """
            Path is a reserved keyword in AWS DynamoDB - we must rename.
        """
        print(get_object(write_event["data"]))
        ret = dynamodb.put_item(
            TableName=f"{table_name}-data",
            Item={
                "path": {"S": path},
                "version": {"N": "0"},
                "data": {"B": base64.b64decode(get_object(write_event["data"]))},
            },
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_exists(#P)",
            ReturnConsumedCapacity="TOTAL",
        )
        print(ret)
        return {"status": "success", "path": path, "version": 0}
    except dynamodb.exceptions.ConditionalCheckFailedException:
        return {"status": "failure", "path": path, "reason": "node_does_not_exist"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}


def delete_node():
    pass


ops: Dict[str, Callable[[dict, bool], dict]] = {
    "create_node": create_node,
    "set_data": set_data,
    "delete_node": delete_node,
    "deregister_session": deregister_session
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
    verbose_output = os.environ["VERBOSE_LOGGING"]
    table_name = os.environ["DYNAMODB_TABLE"]
    print(event)
    processed_events = 0
    for record in events:
        if record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]

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

            ret = ops[op](record["eventID"], write_event, table_name, verbose_output)
            if not ret:
                continue
            notify(write_event, ret)
            print(ret)
            processed_events += 1

    print(f"Successfully processed {processed_events} records out of {len(events)}")
