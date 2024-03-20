import json

from functions.gcp.control.distributor_queue import DistributorQueuePubSub
from functions.gcp.control.datastore import DataStoreStorage as DataStoreDriver
import pytest
import time
import pathlib
import datetime

'''
The nested node test suite is more self contained. It has:
- create and exists node and a child node
- get the child node and set a new value
- delete node(non-empty children), child node and node(empty children)

NOTE: These are all Async
'''
@pytest.fixture(scope="module")
def gcp_globals():
    node_path = "/atest1"
    child_node_path = "/atest1/child1"
    writer_queue = DistributorQueuePubSub("top-cascade-392319", "faasWriter") # worker-queue
    state_storage = DataStoreDriver("wide-axiom-402003", kind_name="faaskeeper-dev-state", database="test2")
    version = "-1"
    child_version = "-1"

    return {
    "node_path": node_path,
        "child_node_path": child_node_path,
        "writer_queue": writer_queue,
        "state_storage": state_storage,
        "version": version,
        "child_version": child_version
    }

def test_create_node_and_child(gcp_globals):
    payload = {
        "op": "create_node",
        "path": gcp_globals["node_path"],
        "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "flags": "0",
    }

    child_payload = {
        "op": "create_node",
        "path": gcp_globals["child_node_path"],
        "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-3",
        "flags": "0",
    }

    data = json.dumps(payload).encode()
    child_data = json.dumps(child_payload).encode()

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    print("create async return", future.result(), datetime.datetime.now())

    child_future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= child_data, ordering_key= "fa3a0cf0")
    print("create child async return", child_future.result(), datetime.datetime.now()) 
    

    # since we dont have gcp as a client doing event scheduling in the client library, we sleep here
    time.sleep(20)
    # check newly create child node
    node_info = gcp_globals["state_storage"].read(gcp_globals["child_node_path"])
    assert node_info.key.name == gcp_globals["child_node_path"]
    assert "cFxidSys" in node_info
    assert "mFxidSys" in node_info
    # get child path
    child = pathlib.Path(gcp_globals["child_node_path"]).name

    parent_node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    assert child in parent_node_info["children"]
    # this will be done in Reader in providers/serialization.py
    gcp_globals["child_version"] = node_info["mFxidSys"][0]
    gcp_globals["version"] = parent_node_info["mFxidSys"][0]
    print("assigned the child version", gcp_globals["child_version"], type(node_info["mFxidSys"]))

def test_set_child(gcp_globals):
    payload = {
        "op": "set_data",
        "path": gcp_globals["child_node_path"],
        "data": "ImRhdGF2ZXIyIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "version":gcp_globals["child_version"]
    }

    data = json.dumps(payload).encode()

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    future.result()

    time.sleep(10)

    # check newly create node
    node_info = gcp_globals["state_storage"].read(gcp_globals["child_node_path"])
    print(node_info.key.name, type(node_info.key.name))
    assert node_info.key.name == gcp_globals["child_node_path"]
    assert "cFxidSys" in node_info
    assert "mFxidSys" in node_info
    gcp_globals["child_version"] = node_info["mFxidSys"][0]
    print("assigned both versions", gcp_globals["child_version"], type(node_info["mFxidSys"]))

def test_delete_node_and_child(gcp_globals):
    print("use version to delete", gcp_globals["version"], type(gcp_globals["version"]))
    payload = {
        "op": "delete_node",
        "path": gcp_globals["node_path"],
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "version": gcp_globals["version"]
    }

    child_payload = {
        "op": "delete_node",
        "path": gcp_globals["child_node_path"],
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-3",
        "version": gcp_globals["child_version"]
    }

    data = json.dumps(payload).encode()
    child_data = json.dumps(child_payload).encode()

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= child_data, ordering_key= "fa3a0cf0")
    print("delete ",future.result())

    # since we dont have gcp as a client doing event scheduling in the client library, we sleep here
    time.sleep(10)
    
    # check newly create node
    node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    print(node_info.key.name, type(node_info.key.name))
    assert node_info.key.name == gcp_globals["node_path"]

    child = pathlib.Path(gcp_globals["child_node_path"]).name
    assert child not in node_info["children"]

    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
    print("delete ",future.result())

    time.sleep(10)
    
    node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
    print(node_info.key.name, type(node_info.key.name))
    assert node_info.key.name == gcp_globals["node_path"]
    assert "cFxidSys" not in node_info