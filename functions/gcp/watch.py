import os
import functions_framework

from faaskeeper.watch import WatchEventType, WatchType
from functions.gcp.model.watches import Watches
from functions.aws.notify import notify

verbose = bool(os.environ["VERBOSE"])
deployment_name = f"faaskeeper-{os.environ['DEPLOYMENT_NAME']}"
region = os.environ["AWS_REGION"]
region_watches = Watches(deployment_name, region)

@functions_framework.http
def handler(event: dict, context: dict):

    try:
        watch_event = WatchEventType(event["event"])
        watch_type = WatchType(event["type"])
        timestamp = event["timestamp"]
        path = event["path"]

        watches_to_retain = []
        watches = region_watches.get_watches(path, [watch_type])
        if len(watches):
            for client in watches[0][1]:
                version = int(client[0])
                if version >= timestamp:
                    if verbose:
                        print(f"Retaining watch with timestamp {version}")
                    watches_to_retain.append(client)
                else:
                    client_ip = client[1]
                    client_port = int(client[2])
                    if verbose:
                        print(f"Notify client at {client_ip}:{client_port}")
                    notify(
                        client_ip,
                        client_port,
                        {
                            "watch-event": watch_event.value,
                            "timestamp": timestamp,
                            "path": path,
                        },
                    )
        return True
    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()
        return False