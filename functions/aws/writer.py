import json
import logging
from typing import Optional

from faaskeeper.stats import StorageStatistics
from functions.aws.config import Config
from functions.aws.control.channel import Client
from functions.aws.operations import Executor
from functions.aws.operations import builder as operations_builder
from functions.aws.stats import TimingStatistics

config = Config.instance()
timing_stats = TimingStatistics.instance()


def execute_operation(op_exec: Executor, client: Client) -> Optional[dict]:

    try:

        status, ret = op_exec.lock_and_read(config.system_storage)
        if not status:
            return ret

        # FIXME: revrse the order here
        status, ret = op_exec.commit_and_unlock(config.system_storage)
        if not status:
            return ret

        assert config.distributor_queue
        op_exec.distributor_push(client, config.distributor_queue)

        return ret

    except Exception:
        # Report failure to the user
        logging.error("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}


def get_object(obj: dict):
    return next(iter(obj.values()))


def handler(event: dict, context):

    events = event["Records"]
    logging.info(f"Begin processing {len(events)} events")
    processed_events = 0
    StorageStatistics.instance().reset()
    for record in events:

        # FIXME: abstract away, hide the DynamoDB conversion
        if "dynamodb" in record and record["eventName"] == "INSERT":
            write_event = record["dynamodb"]["NewImage"]
            event_id = record["eventID"]

        elif "body" in record:
            write_event = json.loads(record["body"])
            if "data" in record["messageAttributes"]:
                write_event["data"] = {
                    "B": record["messageAttributes"]["data"]["binaryValue"]
                }
            event_id = record["attributes"]["MessageDeduplicationId"]
            write_event["timestamp"] = {"S": event_id}
        else:
            raise NotImplementedError()

        client = Client.deserialize(write_event)

        # FIXME: hide DynamoDB serialization somewhere else
        parsed_event = {x: get_object(y) for x, y in write_event.items()}
        op = parsed_event["op"]

        executor, error = operations_builder(op, event_id, parsed_event)
        if executor is None:
            config.client_channel.notify(client, error)
            continue

        ret = execute_operation(executor, client)

        if ret:
            if ret["status"] == "failure":
                logging.error(f"Failed processing write event {event_id}: {ret}")
            else:
                processed_events += 1
            config.client_channel.notify(client, ret)
            continue
        else:
            processed_events += 1

        if timing_stats.repetitions % 100 == 0:
            timing_stats.print()

    print(f"Successfully processed {processed_events} records out of {len(events)}")
    print(
        f"Request: {context.aws_request_id} "
        f"Read: {StorageStatistics.instance().read_units}\t"
        f"Write: {StorageStatistics.instance().write_units}"
    )
