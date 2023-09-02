import functions_framework
import base64
import json
import time

from typing import Dict, List, Set
from concurrent.futures import Future, ThreadPoolExecutor

from functions.gcp.control.distributor_events import DistributorEventType, builder
from faaskeeper.version import SystemCounter
from faaskeeper.watch import WatchType, WatchEventType
from functions.gcp.control.channel import Client
from functions.gcp.stats import TimingStatistics
from functions.gcp.config import Config
from functions.gcp.model.watches import Watches
from functions.cloud_providers import CLOUD_PROVIDER

regions = ["us-central1"]

region_watches = {}
epoch_counters: Dict[str, Set[str]] = {}

config = Config.instance(False)

for r in regions:
    # region_watches[r] = Watches(config.deployment_name, r)
    epoch_counters[r] = set()

timing_stats = TimingStatistics.instance()

executor = ThreadPoolExecutor(max_workers=2 * len(regions))

def launch_watcher(region: str, json_in: dict):
    pass

# Register an HTTP function with the Functions Framework
# on GCP, it is named main.py
@functions_framework.http
def handler(request):
    # Your code here
    request_json = request.get_json(silent=True)
    request_args = request.args

    watches_submitters: List[Future] = []
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")

    # no datastore trigger added

    # trigger by pub/sub subscriber through push substription to ensure message ordering.
    write_event = json.loads(record)
    print('distributor |', write_event)
    event_type = DistributorEventType(int(write_event["type"]))
    # print( "writer |",write_event["sequence_timestamp"], int(write_event["sequence_timestamp"].split(".")[0]))
    counter: SystemCounter = SystemCounter.from_raw_data([int(write_event["sequence_timestamp"])])

    try:
        client = Client.deserialize(write_event)
        print('distributor |', client)
        operation = builder(counter, event_type, write_event, CLOUD_PROVIDER.GCP)
        print('distributor |', operation)
        begin_write = time.time()
        # write new data
        for r in regions: # we do not consider regions for now, because
            ret = operation.execute(
                config.system_storage, config.user_storage, epoch_counters[r]
            )
        end_write = time.time()
        timing_stats.add_result("write", end_write - begin_write)

        # start watch delivery
        for r in regions:
            if event_type == DistributorEventType.SET_DATA:
                watches = {
                    "watch-event": WatchEventType.NODE_DATA_CHANGED.value, 
                    "path":operation.node.path,
                    "timestamp": operation.node.modified.system.sum # modified timestamp
                }
                watches_submitters.append(
                   executor.submit(launch_watcher, r, watches)
                )
            # FIXME: other watchers
            # FIXME: reenable submission
            # Query watches from DynaamoDB to decide later
            # if they should even be scheduled.
            region_watches[r].query_watches(operation.node.path, [WatchType.GET_DATA])
        
        for r in regions: # we do not consider regions for now
            epoch_counters[r].update(operation.epoch_counters())

        if ret:
            # notify client about success
            config.client_channel.notify(
                client,
                ret,
            )
            # processed_events += 1
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