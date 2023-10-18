import functions_framework
import base64
import logging
import json
import hashlib
import time
import os

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, List, Set
from functions.gcp.config import Config
from functions.gcp.control.channel import Client
from functions.gcp.operations import builder as operations_builder, Executor
from functions.gcp.control.gcloud_function import CloudFunction
from functions.gcp.model.watches import Watches
from faaskeeper.watch import WatchEventType, WatchType
from functions.gcp.notify import notify
from faaskeeper.version import SystemCounter
from functions.gcp.stats import TimingStatistics
from functions.gcp.control.distributor_events import DistributorEvent, DistributorEventType, builder
from functions.gcp.cloud_providers import CLOUD_PROVIDER

config = Config.instance()

regions = ["us-central1"]

region_clients: Dict[str, CloudFunction] = {}
region_watches: Dict[str, Watches] = {}
epoch_counters: Dict[str, Set[str]] = {}

for r in regions:
    region_watches[r] = Watches(os.environ['PROJECT_ID'], os.environ['DB_NAME'], config.deployment_name, r)
    epoch_counters[r] = set()
    region_clients[r] = CloudFunction(r, os.environ["PROJECT_ID"])

timing_stats = TimingStatistics.instance()

executor = ThreadPoolExecutor(max_workers=2 * len(regions))

verbose = bool(os.environ["VERBOSE"])

def execute_operation(op_exec: Executor, client: Client) -> Optional[dict]:
    try:
        
        status, ret = op_exec.lock_and_read(config.system_storage)
        if not status: # status == False, node or parent node may not exist
            return ret # error message
        
        assert config.distributor_queue
        op_exec.distributor_push(client, config.distributor_queue)

        # TODO: in gcp for now , we now let distributor do the commit work
        # status, ret = op_exec.commit_and_unlock(config.system_storage)
        # if not status: # status == False
        #     return ret
        
        return ret

    except Exception:
        # Report failure to the user
        logging.error("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}

@functions_framework.http
def writer_handler(request):
    request_json = request.get_json(silent=True)
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")
    write_event = json.loads(record)

    event_id = request_json["message"]["message_id"]
    write_event["timestamp"] = request_json["message"]["publish_time"]

    client = Client.deserialize(write_event)
    op = write_event["op"]

    executor, error = operations_builder(op, event_id, write_event)
    print("writer |",executor, executor.event_id, executor._op.path)
    ret = execute_operation(executor, client)

    if ret:
        if ret["status"] == "failure":
            logging.error(f"Failed processing write event {event_id}: {ret}")

        config.client_channel.notify(client, ret)

    return 'OK'

def launch_watcher(operation: DistributorEvent, region: str, json_in: dict):
    """
    (1) Submit watcher
    (2) Wait for completion
    (3) Remove ephemeral counter.
    """

    is_delivered = region_clients[region].invoke(
        FunctionName=f"{config.deployment_name}-watch",
        Payload=json.dumps(json_in).encode(),
    )
    is_delivered = bool(is_delivered)

    if is_delivered:
        hashed_path = hashlib.md5(json_in["path"].encode()).hexdigest()
        timestamp = json_in["timestamp"]
        watch_type = json_in["type"]

        epoch_counters[r].remove(f"{hashed_path}_{watch_type}_{timestamp}")
        operation.update_epoch_counters(config.user_storage, epoch_counters[r])
        return True
    return False

@functions_framework.http
def distributor_handler(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    watches_submitters: List[Future] = []
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")

    write_event = json.loads(record)
    event_type = DistributorEventType(int(write_event["type"]))

    publish_time = datetime.strptime(request_json["message"]["publishTime"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d%H%M%S%f")
    counter: SystemCounter = SystemCounter.from_raw_data([int(publish_time[:-3])])
    try:
        client = Client.deserialize(write_event)
        operation = builder(event_type, write_event, CLOUD_PROVIDER.GCP)
        begin_write = time.time()
        for r in regions:
            ret = operation.execute(
                config.system_storage, config.user_storage, epoch_counters[r], counter
            )
        end_write = time.time()
        timing_stats.add_result("write", end_write - begin_write)

        # start watch delivery
        for r in regions:
            for watch in operation.generate_watches_event(region_watches[r]):
                watch_dict = {
                    "event": watch.watch_event_type,
                    "type": watch.watch_type,
                    "path": watch.node_path,
                    "timestamp": watch.mFxidSys,
                }

                watches_submitters.append(
                    executor.submit(launch_watcher, operation, r, watch_dict) # watch: {DistributorEvent, watchType, timestamp, path}
                )

        for r in regions:
            epoch_counters[r].update(operation.epoch_counters())

        if ret:
            # notify client about success
            config.client_channel.notify(
                client,
                ret,
            )
            
        else:
            config.client_channel.notify(
                client,
                {"status": "failure", "reason": "distributor failure"},
            )

    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()
        config.client_channel.notify(
            client,
            {"status": "failure", "reason": "distributor failure"},
        )
    for f in watches_submitters:
        f.result()

    # Return an HTTP response
    return 'OK'

@functions_framework.http
def watch_handler(request):
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
    
# Register a dummy function for meeting the requirement of deploying a function in functions section in serverless template
@functions_framework.http
def dummy_http_function(request):
  # Your code here

  # Return an HTTP response
  return 'OK'