import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, List, Set

import boto3

from faaskeeper.stats import StorageStatistics
from faaskeeper.version import SystemCounter
from faaskeeper.watch import WatchType
from functions.aws.config import Config
from functions.aws.control.channel import Client
from functions.aws.control.distributor_events import DistributorEventType, builder
from functions.aws.model.watches import Watches
from functions.aws.stats import TimingStatistics

mandatory_event_fields = [
    "op" "path",
    "session_id",
    "version",
    "sourceIP",
    "sourcePort",
    "data",
]

config = Config.instance(False)

"""
    The data received from the queue includes:
    - client IP and port
    - updates

    We support the following cases:
    - create_node - writing user node (no data) and writing children in parent node
    - set_data - update counter and data
    - delete_node - delete node and overwrite parent nodes
"""

# FIXME: configure
regions = ["us-east-1"]
# verbose_output = config.verbose
# verbose_output = False
# FIXME: proper data structure
region_clients = {}
region_watches = {}
epoch_counters: Dict[str, Set[str]] = {}
for r in regions:
    region_clients[r] = boto3.client("lambda", region_name=r)
    region_watches[r] = Watches(config.deployment_name, r)
    epoch_counters[r] = set()
executor = ThreadPoolExecutor(max_workers=2 * len(regions))


def get_object(obj: dict):
    return next(iter(obj.values()))


def launch_watcher(region: str, json_in: dict):
    """
    (1) Submit watcher
    (2) Wait for completion
    (3) Remove ephemeral counter.
    """
    # FIXME process result
    region_clients[region].invoke(
        FunctionName=f"{config.deployment_name}-watch",
        InvocationType="RequestResponse",
        Payload=json.dumps(json_in).encode(),
    )


# def query_watch_id(region: str, node_path: str):
#    return region_watches[region].get_watch_counters(node_path)

timing_stats = TimingStatistics.instance()


def handler(event: dict, context):

    events = event["Records"]
    logging.info(f"Begin processing {len(events)} events")

    processed_events = 0
    StorageStatistics.instance().reset()
    try:
        begin = time.time()
        watches_submitters: List[Future] = []
        for record in events:

            counter: SystemCounter
            if "dynamodb" in record and record["eventName"] == "INSERT":
                write_event = record["dynamodb"]["NewImage"]
                event_type = DistributorEventType(int(write_event["type"]["N"]))

                # when launching from a trigger, the binary vlaue is not
                # automatically base64 decoded
                # however, we can't put base64 encoded data to boto3:
                # it ALWAYS applies encoding,
                # regardless of the format of data
                # https://github.com/boto/boto3/issues/3291
                # https://github.com/aws/aws-cli/issues/1097
                # if "data" in write_event:
                #    write_event["data"]["B"] = base64.b64decode(
                #       write_event["data"]["B"]
                #    )

            elif "body" in record:
                write_event = json.loads(record["body"])
                event_type = DistributorEventType(int(write_event["type"]["N"]))
                if "data" in record["messageAttributes"]:
                    write_event["data"] = {
                        "B": record["messageAttributes"]["data"]["binaryValue"]
                    }
                counter = SystemCounter.from_raw_data(
                    record["attributes"]["SequenceNumber"]
                )
            else:
                raise NotImplementedError()

            """
                (1) Parse the incoming event.
                (2) Extend the epoch counter.
                (3) Apply the update.
                (4) Distribute the watch notifications, if needed.
                (5) Notify client.
            """

            try:

                client = Client.deserialize(write_event)

                operation = builder(counter, event_type, write_event)

                begin_write = time.time()
                # write new data
                for r in regions:
                    ret = operation.execute(
                        config.system_storage, config.user_storage, epoch_counters[r]
                    )
                end_write = time.time()
                timing_stats.add_result("write", end_write - begin_write)

                begin_watch = time.time()
                # start watch delivery
                for r in regions:
                    # if event_type == DistributorEventType.SET_DATA:
                    #    watches_submitters.append(
                    #        executor.submit(launch_watcher, r, watches)
                    #    )
                    # FIXME: other watchers
                    # FIXME: reenable submission
                    # Query watches from DynaamoDB to decide later
                    # if they should even be scheduled.
                    region_watches[r].query_watches(
                        operation.node.path, [WatchType.GET_DATA]
                    )

                end_watch = time.time()
                timing_stats.add_result("watch", end_watch - begin_watch)

                for r in regions:
                    epoch_counters[r].update(operation.epoch_counters())
                begin_notify = time.time()
                if ret:
                    # notify client about success
                    config.client_channel.notify(
                        client,
                        ret,
                    )
                    processed_events += 1
                else:
                    config.client_channel.notify(
                        client,
                        {"status": "failure", "reason": "distributor failure"},
                    )
                end_notify = time.time()
                timing_stats.add_result("notify", end_notify - begin_notify)

            except Exception:
                print("Failure!")
                import traceback

                traceback.print_exc()
                config.client_channel.notify(
                    client,
                    {"status": "failure", "reason": "distributor failure"},
                )
        begin_watch_wait = time.time()
        for f in watches_submitters:
            f.result()
        end_watch_wait = time.time()
        timing_stats.add_result("watch_notify", end_watch_wait - begin_watch_wait)
        end = time.time()
        timing_stats.add_result("total", end - begin)
        timing_stats.add_repetition()

        if timing_stats.repetitions % 100 == 0:
            timing_stats.print()

    except Exception:
        print("Failure!")
        import traceback

        traceback.print_exc()

    # print(f"Successfully processed {processed_events} records out of {len(events)}")
    print(
        f"Request: {context.aws_request_id} "
        f"Read: {StorageStatistics.instance().read_units}\t"
        f"Write: {StorageStatistics.instance().write_units}"
    )
