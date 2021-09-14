import json
import os
import socket
from datetime import datetime

from faaskeeper.watch import WatchEventType, WatchType
from functions.aws.model.watches import Watches

verbose = bool(os.environ["VERBOSE"])
deployment_name = f"faaskeeper-{os.environ['DEPLOYMENT_NAME']}"
region = os.environ["AWS_REGION"]


def handler(event: dict, context: dict):

    start = datetime.now()
    if verbose:
        print(f"Begin heartbeat at {start}")

    try:
        pass
    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()
