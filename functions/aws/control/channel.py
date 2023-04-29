import json
import logging
import os
import socket
from abc import ABC, abstractmethod
from typing import Optional

import boto3
from botocore.exceptions import ClientError


class ClientChannel(ABC):
    @abstractmethod
    def notify(self, session_id: str, event: str, write_event: dict, ret: dict):
        pass

class ClientConfig:
    session_id: str
    ip: Optional[str] = None
    port: Optional[str] = None

class ClientChannelTCP(ClientChannel):
    def __init__(self):
        self._sockets = {}

    def _get_socket(self, user: str, write_event: dict):

        sock = self._sockets.get(user, None)

        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(2)
                source_ip = get_object(write_event["sourceIP"])
                source_port = int(get_object(write_event["sourcePort"]))
                sock.connect((source_ip, source_port))
                logging.info(f"Connected to {source_ip}:{source_port}")

                self._sockets[user] = sock
            except socket.timeout:
                logging.error(f"Connection to client {source_ip}:{source_port} failed!")
            except OSError as e:
                logging.error(
                    f"Connection to client {source_ip}:{source_port} failed! "
                    f"Reason {e.strerror}"
                )

        return sock

    def notify(self, user: str, event: str, write_event: dict, ret: dict):
        sock = self._get_socket(user, write_event)
        if sock is None:
            logging.error(f"Notification of client {user} failed!")
            return

        try:
            sock.sendall(
                json.dumps(
                    {**ret, "event": event}
                ).encode()
            )
        except socket.timeout:
            logging.error(f"Notification of client {user} failed!")


class ClientChannelSQS(ClientChannel):
    def __init__(self):
        self._sqs = boto3.client("sqs", region_name=os.environ["AWS_REGION"])
        self._queue_name = f"faaskeeper-{os.environ['DEPLOYMENT_NAME']}-client-sqs"

        try:
            self._queue_url = self._sqs.get_queue_url(QueueName=self._queue_name)[
                "QueueUrl"
            ]
        except ClientError as error:
            logging.exception(f"Couldn't get queue named {self._queue_name}")
            raise error

    def notify(self, user: str, event: str, write_event: dict, ret: dict):
        try:
            self._sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps({**ret, "event": event}),
            )
        except ClientError as error:
            logging.error(f"Notification of client {user} failed!")
            raise error
