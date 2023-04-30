import json
import logging
import os
import socket
from abc import ABC, abstractmethod
from typing import Optional

import boto3
from botocore.exceptions import ClientError


def get_object(obj: dict):
    return next(iter(obj.values()))


# FIXME: in future we should have a generic class for request
# and support for DynamodB items
class Client:
    def __init__(self):
        self.session_id: str
        self.timestamp: str
        self.sourceIP: Optional[str] = None
        self.sourcePort: Optional[str] = None

    @staticmethod
    def deserialize(dct: dict):

        client = Client()
        client.session_id = get_object(dct["session_id"])
        client.timestamp = get_object(dct["timestamp"])
        if "sourceIP" in dct:
            client.sourceIP = get_object(dct["sourceIP"])
        if "sourcePort" in dct:
            client.sourcePort = get_object(dct["sourcePort"])

        return client

    def serialize(self) -> dict:
        data = {"session_id": self.session_id, "timestamp": self.timestamp}
        if self.sourceIP is not None:
            data["sourceIP"] = self.sourceIP
        if self.sourcePort is not None:
            data["sourcePort"] = self.sourcePort
        return data


class ClientChannel(ABC):
    @abstractmethod
    def notify(self, user: Client, ret: dict):
        pass


class ClientChannelTCP(ClientChannel):
    def __init__(self):
        self._sockets = {}

    def _get_socket(self, user: Client):

        sock = self._sockets.get(user.session_id, None)

        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(2)
                source_ip = user.sourceIP
                assert user.sourcePort
                source_port = int(user.sourcePort)
                sock.connect((source_ip, source_port))
                logging.info(f"Connected to {source_ip}:{source_port}")

                self._sockets[user.session_id] = sock
            except socket.timeout:
                logging.error(f"Connection to client {source_ip}:{source_port} failed!")
            except OSError as e:
                logging.error(
                    f"Connection to client {source_ip}:{source_port} failed! "
                    f"Reason {e.strerror}"
                )

        return sock

    def notify(self, user: Client, ret: dict):

        sock = self._get_socket(user)
        if sock is None:
            logging.error(f"Notification of client {user} failed!")
            return

        try:
            sock.sendall(json.dumps({**ret, "event": user.timestamp}).encode())
        except socket.timeout:
            logging.error(f"Notification of client {user} failed!")


class ClientChannelSQS(ClientChannel):
    def __init__(self):
        self._sqs = boto3.client("sqs", region_name=os.environ["AWS_REGION"])
        self._queues = {}
        self._deployment = os.environ["DEPLOYMENT_NAME"]

    def _get_queue(self, user: Client) -> str:

        queue = self._queues.get(user.session_id, None)

        if queue is None:

            queue_name = f"faaskeeper-{self._deployment}-client-sqs"
            try:
                queue = self._sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            except ClientError as error:
                logging.exception(f"Couldn't get queue named {queue_name}")
                raise error

            self._queues[user.session_id] = queue

        return queue

    def notify(self, user: Client, ret: dict):

        try:
            queue = self._get_queue(user)
            self._sqs.send_message(
                QueueUrl=queue,
                MessageBody=json.dumps({**ret, "event": user.timestamp}),
            )
        except ClientError as error:
            logging.error(f"Notification of client {user} failed!")
            raise error
