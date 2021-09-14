import os
from datetime import datetime

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
