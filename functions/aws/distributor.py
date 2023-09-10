import json
import logging
import time
import hashlib
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, List, Set

import boto3

from faaskeeper.stats import StorageStatistics
from faaskeeper.version import SystemCounter
from functions.aws.config import Config
from functions.aws.control.channel import Client
from functions.aws.control.distributor_events import DistributorEvent, DistributorEventType, builder
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
region_watches: Dict[str, Watches] = {}
epoch_counters: Dict[str, Set[str]] = {}
for r in regions:
    region_clients[r] = boto3.client("lambda", region_name=r)
    region_watches[r] = Watches(config.deployment_name, r)
    epoch_counters[r] = set()
executor = ThreadPoolExecutor(max_workers=2 * len(regions))


def get_object(obj: dict):
    return next(iter(obj.values()))


def launch_watcher(operation: DistributorEvent, region: str, json_in: dict):
    """
    (1) Submit watcher
    (2) Wait for completion
    (3) Remove ephemeral counter.
    """
    is_delivered = region_clients[region].invoke(
        FunctionName=f"{config.deployment_name}-watch",
        InvocationType="RequestResponse",
        Payload=json.dumps(json_in).encode(),
    )

    if is_delivered:
        hashed_path = hashlib.md5(json_in["path"].encode()).hexdigest()
        timestamp = json_in["timestamp"]
        watch_type = json_in["type"]
        
        # pop the pending watch: update epoch counters for the node and parent in user storage.
        epoch_counters[r].remove(f"{hashed_path}_{watch_type}_{timestamp}")
        operation.update_epoch_counters(config.user_storage, epoch_counters[r])
        return True
    return False

timing_stats = TimingStatistics.instance()


def handler(event: dict, context):
    events = event["Records"]
    logging.info(f"Begin processing {len(events)} events")

    processed_events = 0
    StorageStatistics.instance().reset()
    try:
        if config.benchmarking:
            begin = time.time()
        watches_submitters: List[Future] = []
        for record in events:

            if config.benchmarking:
                begin_parse = time.time()
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
                    [int(record["attributes"]["SequenceNumber"])]
                )
            else:
                raise NotImplementedError()
            if config.benchmarking:
                end_parse = time.time()
                timing_stats.add_result("parse", end_parse - begin_parse)

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

                if config.benchmarking:
                    begin_watch = time.time()
                # start watch delivery
                for r in regions: # deliver watch concurrently
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
                if config.benchmarking:
                    end_watch = time.time()
                    timing_stats.add_result("watch_query", end_watch - begin_watch)

                for r in regions:
                    epoch_counters[r].update(operation.epoch_counters())
                if config.benchmarking:
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
                if config.benchmarking:
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
        if config.benchmarking:
            begin_watch_wait = time.time()
        for f in watches_submitters:
            f.result()
        if config.benchmarking:
            end_watch_wait = time.time()
            timing_stats.add_result("watch_notify", end_watch_wait - begin_watch_wait)
            end = time.time()
            timing_stats.add_result("total", end - begin)
            timing_stats.add_repetition()

        if (
            config.benchmarking
            and timing_stats.repetitions % config.benchmarking_frequency == 0
        ):
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
