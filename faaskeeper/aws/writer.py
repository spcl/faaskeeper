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

"""
    The function has the following responsibilities:
    1) Create new node, returning success or failure if the node exists
    2) Set-up ACL permission on the node.
    3) Add the list to user's nodelist in case of an ephemeral node.
    4) Create sequential node by appending newest version.
    5) Create parents nodes to make sure the entire path exists.
    6) Look-up watches in a seperate table.
"""


def create_node(write_event: dict, table_name: str, verbose_output: bool) -> dict:

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
                "_path": {"S": path},
                "version": {"N": "0"},
                "data": {"B": get_object(write_event["data"])},
            },
            ExpressionAttributeNames={"#P": "path"},
            ConditionExpression="attribute_not_exists(#P)",
            ReturnConsumedCapacity='TOTAL'
        )
        print(ret)
        return {"status": "success", "path": path, "version": 0}
    except client.exceptions.ConditionalCheckFailedException as e:
        return {"status": "failure", "reason": f"Node {path} exists!"}
    except Exception as e:
        # Report failure to the user
        print("Failure!")
        print(e)
        return {"status": "failure", "reason": "unknown"}


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


def get_object(obj: dict):
    return next(iter(obj.values()))


def handler(event: dict, context: dict):

    events = event["Records"]
    verbose_output = os.environ["VERBOSE_LOGGING"]
    table_name = os.environ["DYNAMODB_TABLE"]
    print(event)
    processed_events = 0
    for record in events:
        if record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]

            """
                Handle malformed events correctly.
            """
            if any(k not in write_event.keys() for k in mandatory_event_fields):
                if verbose_output:
                    print(
                        "Incorrect event with ID {id}, timestamp {timestamp}".format(
                            id=record["eventID"], timestamp=write_event["timestamp"]
                        )
                    )
                continue
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

            ret = ops[op](write_event, table_name, verbose_output)
            print(ret)
            processed_events += 1

    print(f"Successfully processed {processed_events} records out of {len(events)}")
