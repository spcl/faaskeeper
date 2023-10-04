import json

from functions.gcp.control.distributor_queue import DistributorQueuePubSub
from functions.gcp.control.datastore import DataStoreStorage as DataStoreDriver
import pytest
import time

# make the test more self contained.
# node.py (create, exist, get and set, delete)

'''
The node test suite is more self contained. It has:
- create node and exist node
- get node and set a new value
- delete node(empty children)
'''
@pytest.fixture(scope="module")
def gcp_globals():
    node_path = "/atest1"
    writer_queue = DistributorQueuePubSub("top-cascade-392319", "faasWriter") # worker-queue
    state_storage = DataStoreDriver(kind_name="faaskeeper-dev-state")
    version = "-2"

    return {
        "node_path": node_path,
        "writer_queue": writer_queue,
        "state_storage": state_storage,
        "version": version
    }

def test_create_node(gcp_globals):
    payload = {
        "op": "create_node",
        "path": gcp_globals["node_path"],
        "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "flags": "0",
    }

        
    payloadb2 = {
        "op": "create_node",
        "path": "/atest2",
        "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-4",
        "flags": "0",
    }

    data = json.dumps(payload).encode()
    datab2 = json.dumps(payloadb2).encode()
    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    futureb2 = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= datab2, ordering_key= "fa3a0cf0")
    
    future.result()

    # since we dont have gcp as a client doing event scheduling in the client library, we sleep here
    time.sleep(20)
    
    # check newly create node
    node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    assert node_info.key.name == gcp_globals["node_path"]
    assert "cFxidSys" in node_info
    assert "mFxidSys" in node_info
    # this will be done in Reader in providers/serialization.py
    gcp_globals["version"] = node_info["mFxidSys"][0]
    print("assigned the version", gcp_globals["version"], type(node_info["mFxidSys"]))

def test_set_node(gcp_globals):
    payload = {
        "op": "set_data",
        "path": gcp_globals["node_path"],
        "data": "ImRhdGF2ZXIyIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "version":gcp_globals["version"]
    }

    data = json.dumps(payload).encode()

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    future.result()

    time.sleep(10)

    # check newly create node
    node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    assert node_info.key.name == gcp_globals["node_path"]
    assert "cFxidSys" in node_info
    assert "mFxidSys" in node_info
    assert node_info["mFxidSys"][0] != gcp_globals["version"]
    # this should create error in delete as version is not updated.
    # check local code and online gcloud func
    gcp_globals["version"] = node_info["mFxidSys"][0]
    print("havent assigned the version", node_info["mFxidSys"][0] ,gcp_globals["version"], type(node_info["mFxidSys"]))

def test_delete_node(gcp_globals):
    print("use version to delete", gcp_globals["version"], type(gcp_globals["version"]))
    payload = {
        "op": "delete_node",
        "path": gcp_globals["node_path"],
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "version": gcp_globals["version"]
    }
    data = json.dumps(payload).encode()

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    future.result()

    # since we dont have gcp as a client doing event scheduling in the client library, we sleep here
    time.sleep(10)
    
    # check newly create node
    node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    assert node_info.key.name == gcp_globals["node_path"]
    assert "cFxidSys" not in node_info
