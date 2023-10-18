import functions_framework
import base64
import logging
import json

from typing import Optional
from functions.gcp.config import Config
from functions.gcp.control.channel import Client
from functions.gcp.operations import builder as operations_builder, Executor

config = Config.instance()

def execute_operation(op_exec: Executor, client: Client) -> Optional[dict]:
    try:
        
        status, ret = op_exec.lock_and_read(config.system_storage)
        if not status: # status == False, node or parent node may not exist
            return ret # error message
        
        assert config.distributor_queue
        op_exec.distributor_push(client, config.distributor_queue)

        # TODO: in gcp for now , we now let distributor do the commit work
        status, ret = op_exec.commit_and_unlock(config.system_storage)
        if not status: # status == False
            return ret
        
        return ret

    except Exception:
        # Report failure to the user
        logging.error("Failure!")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "reason": "unknown"}

# Register an HTTP function with the Functions Framework
@functions_framework.http
def handler(request):
    request_json = request.get_json(silent=True)
    record = base64.b64decode(request_json["message"]["data"]).decode("utf-8")
    write_event = json.loads(record)

    event_id = request_json["message"]["message_id"]

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