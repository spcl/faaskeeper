import functions_framework
import base64
import json
from os import environ
from faaskeeper.watch import WatchEventType, WatchType
from functions.gcp.model.watches import Watches
from functions.aws.notify import notify

verbose = bool(environ["VERBOSE"])
deployment_name = f"faaskeeper-{environ['DEPLOYMENT_NAME']}"
region_watches = {}
regions = ["us-central1"]
for r in regions:
    region_watches[r] = Watches(environ['PROJECT_ID'], environ['DB_NAME'], deployment_name, r)

@functions_framework.http
def handler(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")
    event = json.loads(record)
    try:
        watch_event = WatchEventType(event["event"])
        watch_type = WatchType(event["type"])
        timestamp = event["timestamp"]
        path = event["path"]

        watches_to_retain = []
        for r in regions:
            watches = region_watches[r].get_watches(path, [watch_type])
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
        return 'True'
    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()
        return 'False'