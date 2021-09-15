import json
import os
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict

from boto3.dynamodb.types import TypeDeserializer

from functions.aws.model.users import Users

# verbose = bool(os.environ["VERBOSE"])
verbose = False
# max_users = int(os.environ["HEARTBEAT_MAX_USERS"])
max_users = 64
deployment_name = f"faaskeeper-{os.environ['DEPLOYMENT_NAME']}"
region = os.environ["AWS_REGION"]
users_table = Users(deployment_name, region)
deserializer = TypeDeserializer()
executor = ThreadPoolExecutor(max_workers=max_users)

sockets: Dict[str, socket.socket] = {}


def notify(addr: str, session: str, s=None):
    # FIXME handle removal
    if s:
        if verbose:
            print(f"Old socket for {addr}")
        try:
            s.sendall(json.dumps({"type": "heartbeat", "session_id": session}).encode())
            if verbose:
                print(f"Send to {addr}")
            data = s.recv(1024)
            if verbose:
                print(f"Received from {addr}, {data}")
        except socket.timeout:
            print(f"Notification of client {addr} failed!")
            return False, None, None, None
        return True, None, None, None
    else:
        if verbose:
            print(f"New socket for {addr}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(2)
            addr, port = addr.split(":")
            s.connect((addr, int(port)))
            s.sendall(json.dumps({"type": "heartbeat", "session_id": session}).encode())
            if verbose:
                print(f"Send to {addr}")
            data = s.recv(1024)
            if verbose:
                print(f"Received from {addr}, {data}")
        except socket.timeout:
            print(f"Notification of client {addr} failed!")
            return False, None, None, None
        return True, addr, session, s


def handler(event: dict, context: dict):

    start = datetime.now()
    if verbose:
        print(f"Begin heartbeat at {start}")
    data, read_capacity = users_table.get_users()
    active_clients = 0
    try:
        futures = []
        for user in data:
            address = deserializer.deserialize(user["addr"])
            session_id = deserializer.deserialize(user["user"])
            if f"{address}_{session_id}" in sockets:
                futures.append(
                    executor.submit(notify, address, session_id, sockets[address])
                )
            else:
                futures.append(executor.submit(notify, address, session_id, None))
        for f in futures:
            alive, addr, session, s = f.result()
            active_clients += alive
            if s is not None:
                sockets[f"{addr}_{session}"] = s
    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()

    return {"cost": read_capacity, "active_clients": active_clients}
