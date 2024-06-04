import functions_framework
import base64
import json
import time
import hashlib

from os import environ
from typing import Dict, List, Set
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from functions.gcp.control.gcloud_function import CloudFunction
from functions.gcp.control.distributor_events import DistributorEvent, DistributorEventType, builder
from faaskeeper.version import SystemCounter
from functions.gcp.control.channel import Client
from functions.gcp.stats import TimingStatistics
from functions.gcp.config import Config
from functions.gcp.model.watches import Watches
from functions.gcp.cloud_providers import CLOUD_PROVIDER

regions = ["us-central1"]

region_clients: Dict[str, CloudFunction] = {}
region_watches: Dict[str, Watches] = {}
epoch_counters: Dict[str, Set[str]] = {}

config = Config.instance(False)

for r in regions:
    region_watches[r] = Watches(environ['PROJECT_ID'], environ['DB_NAME'], config.deployment_name, r)
    epoch_counters[r] = set()
    region_clients[r] = CloudFunction(r, environ["PROJECT_ID"])

timing_stats = TimingStatistics.instance()

executor = ThreadPoolExecutor(max_workers=2 * len(regions))

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

# Register an HTTP function with the Functions Framework
@functions_framework.http
def handler(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    watches_submitters: List[Future] = []
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")

    write_event = json.loads(record)
    event_type = DistributorEventType(int(write_event["type"]))

    # multiple
    publish_time = None
    for format in (("%Y-%m-%dT%H:%M:%S.%fZ", "%Y%m%d%H%M%S%f"), ("%Y-%m-%dT%H:%M:%SZ","%Y%m%d%H%M%S%f")):
        try:
            publish_time = datetime.strptime(request_json["message"]["publishTime"], format[0]).strftime(format[1])
        except ValueError:
            pass
    if publish_time is None:
        pt = request_json["message"]["publishTime"]
        raise ValueError(f"Non-valid date format for'{pt}'")
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
        if event_type == DistributorEventType.SET_DATA:
            timing_stats.add_result("write set", end_write - begin_write)
        elif event_type == DistributorEventType.CREATE_NODE:
            timing_stats.add_result("write create", end_write - begin_write)
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