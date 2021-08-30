import base64
import json
import socket
from typing import Callable, Dict, Optional

from functions.aws.config import Config
from functions.aws.model.user_storage import OpResult

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

        # if isinstance(write_event["data"], dict):
        #    parsed_data = "".join([chr(val) for val in data["data"]])
        # else:
        # print(data)
        # print(type(data))
        # parsed_data = base64.b64decode(data)
        """
            Path is a reserved keyword in AWS DynamoDB - we must rename.
        """
        # FIXME: check return value
        ret = config.user_storage.write(path, base64.b64decode(data))

        if ret == OpResult.SUCCESS:
            return {"status": "success", "path": path, "version": 0}
        else:
            return {"status": "failure", "path": path, "reason": "node_exists"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}


def deregister_session(
    id: str, write_event: dict, verbose_output: bool
) -> Optional[dict]:

    session_id = get_object(write_event["session_id"])
    try:
        # TODO: remove ephemeral nodes
        # FIXME: check return value
        config.system_storage.delete(session_id)

        return {"status": "success", "session_id": session_id}
    except config.system_storage.errorSupplier.ResourceNotFoundException:
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

    if not verify_event(id, write_event, verbose_output):
        return None
    # FIXME: version
    # FIXME: full conditional update
    try:
        path = get_object(write_event["path"])
        version = get_object(write_event["version"])
        print(version)
        if verbose_output:
            print(f"Attempting to write data at {path}")
        """
            Path is a reserved keyword in AWS DynamoDB - we must rename.
        """
        print(get_object(write_event["data"]))
        # ret = dynamodb.update_item(
        #    TableName=f"{table_name}-data",
        #    Key={
        #        "path": {"S": path},
        #        # "version": {"N": version},
        #    },
        #    # AttributeUpdates={
        #    #    "data": {
        #    #        "Value": {
        #    #            "B": base64.b64decode(get_object(write_event["data"]))
        #    #        }, #        "Action": { "PUT" }
        #    #    }
        #    # },
        #    # ExpressionAttributeNames={"#P": "path"},
        #    ConditionExpression="(attribute_exists(#P)) and (version = :version)",
        #    UpdateExpression="SET #D = :data ADD version :inc",
        #    ExpressionAttributeNames={"#D": "data", "#P": "path"},
        #    ExpressionAttributeValues={
        #        ":version": {"N": get_object(write_event["version"])},
        #        ":inc": {"N": "1"},
        #        ":data": {"B": base64.b64decode(get_object(write_event["data"]))},
        #    },
        #    ReturnConsumedCapacity="TOTAL",
        # )
        return {"status": "success", "path": path, "version": 0}
    except config.user_storage.errorSupplier.ConditionalCheckFailedException as e:
        print(e)
        return {"status": "failure", "path": path, "reason": "update_failure"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
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
