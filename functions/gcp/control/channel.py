import json
import logging
import socket
from abc import ABC, abstractmethod
from typing import Optional

class Client:
    def __init__(self):
        self.session_id: str
        self.timestamp: str
        self.sourceIP: Optional[str] = None
        self.sourcePort: Optional[str] = None

    @staticmethod
    def deserialize(dct: dict):
        client = Client()
        client.session_id = dct["session_id"]
        client.timestamp = dct["timestamp"]
        if "sourceIP" in dct:
            client.sourceIP = dct["sourceIP"]
        if "sourcePort" in dct:
            client.sourcePort = dct["sourcePort"]

        return client

    def serialize(self) -> dict:
        data = {"session_id": self.session_id, "timestamp": self.timestamp}
        if self.sourceIP is not None:
            data["sourceIP"] = self.sourceIP
        if self.sourcePort is not None:
            data["sourcePort"] = self.sourcePort
        return data
    
    def __str__(self) -> str:
        return f"session id: {self.session_id}, timestamp: {self.timestamp}, sourceIP: {self.sourceIP}, sourcePort: {self.sourcePort}"


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
        except BrokenPipeError:
            print("BrokenPipeError connection closed on the other end")
            logging.error("BrokenPipeError connection closed on the other end")
        except Exception:
            print("General exception")
            logging.error("General exception")
