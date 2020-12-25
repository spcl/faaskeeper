from datetime import datetime
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
    "flags",
    "sourceIP",
    "sourcePort",
    "data",
]
dynamodb = boto3.client("dynamodb")



def handler(event: dict, context: dict):

    print(handler)

    print(f"{str(datetime.now())} Called heartbeat")

