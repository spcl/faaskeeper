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
    writer_queue = DistributorQueuePubSub("wide-axiom-402003", "writer-queue-topic") # worker-queue
    state_storage = DataStoreDriver("wide-axiom-402003", kind_name="faaskeeper-dev-state", database="test2")
    version = "-1"

    return {
        "node_path": node_path,
        "writer_queue": writer_queue,
        "state_storage": state_storage,
        "version": version
    }

# def test_concurrency_create(gcp_globals):
#     rep = 50
#     keys = ["fa3a0cf0", "da3a0sf1", "ec3a0sf1"] # 
#     begin = time.time()
#     for i in range(rep):
#         k = keys[i % len(keys)]
#         print("use", k)
#         # create
#         payload = {
#             "op": "create_node",
#             "path": f"/acon{i}/c{i}", # "path": f"/acon{i}/c{i}",
#             "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
#             "session_id": f"{k}", # is defined in the client library
#             "timestamp": f"{k}-{i}",
#             "flags": "0",
#         }

#         data = json.dumps(payload).encode()
#         future = gcp_globals["writer_queue"].publisher_client.publish(
#             gcp_globals["writer_queue"].topic_path, 
#             data= data, 
#             ordering_key= k)
    
#     cfxid = None
#     while cfxid == None:
#         node_info = gcp_globals["state_storage"].read(f"/acon{rep-1}/c{rep-1}") # f"/acon{rep-1}/c{rep-1}"
#         if node_info and "cFxidSys" in node_info:
#             cfxid = node_info["cFxidSys"][0]
#         time.sleep(1)
    
#     end = time.time()

#     print("total time spent on",rep, "create", end - begin)

# def test_concurrency_set(gcp_globals):
#     rep = 50
#     vs = []
#     for i in range(rep):
#         res = gcp_globals["state_storage"].read(f"/acon{i}/c{i}")
#         vs.append(res["mFxidSys"][0])
    
#     keys = ["fa3a0cf0", "da3a0sf1", "ec3a0sf1", "ga3a0sf1"] # 

#     begin = time.time()
#     for i in range(rep):
#         k = keys[i % len(keys)]
#         print("use", k)
#         payload = {
#             "op": "set_data",
#             "path": f"/acon{i}/c{i}", # "path": f"/acon{i}/c{i}",
#             "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
#             "session_id": f"{k}", # is defined in the client library
#             "timestamp": f"{k}-{i}",
#             "version":vs[i]
#         }

#         data = json.dumps(payload).encode()
#         future = gcp_globals["writer_queue"].publisher_client.publish(
#             gcp_globals["writer_queue"].topic_path, 
#             data= data, 
#             ordering_key= k)
    
#     mfxid = vs[-1]
#     while mfxid == vs[-1]:
#         node_info = gcp_globals["state_storage"].read(f"/acon{rep-1}/c{rep-1}") # f"/acon{rep-1}/c{rep-1}"
#         if node_info and "mFxidSys" in node_info:
#             mfxid = node_info["mFxidSys"][0]
#         time.sleep(1)

#     end = time.time()

#     #FIXME: maybe test read somehow concurrently as well?
#     print("total time spent on", rep, "set", end-begin)
    

def test_create_node(gcp_globals):
    payload = {
        "op": "create_node",
        "path": gcp_globals["node_path"],
        "data": "ImRhdGF2ZXIxIg==",#base64.b64encoded
        "session_id": "fa3a0cf0", # is defined in the client library
        "timestamp": "fa3a0cf0-2",
        "flags": "0",
        "sourcePort": 5000,
        "sourceIP": "127.0.0.0"
    }

    data = json.dumps(payload).encode()
    future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
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

# def test_set_node(gcp_globals):
#     payload = {
#         "op": "set_data",
#         "path": gcp_globals["node_path"],
#         "data": "ImRhdGF2ZXIyIg==",#base64.b64encoded
#         "session_id": "fa3a0cf0", # is defined in the client library
#         "timestamp": "fa3a0cf0-2",
#         "version":gcp_globals["version"]
#     }

#     data = json.dumps(payload).encode()

#     future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
#     future.result()

#     time.sleep(10)

#     # check newly create node
#     node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
#     assert node_info.key.name == gcp_globals["node_path"]
#     assert "cFxidSys" in node_info
#     assert "mFxidSys" in node_info
#     assert node_info["mFxidSys"][0] != gcp_globals["version"]
#     # this should create error in delete as version is not updated.
#     # check local code and online gcloud func
#     gcp_globals["version"] = node_info["mFxidSys"][0]
#     print("havent assigned the version", node_info["mFxidSys"][0] ,gcp_globals["version"], type(node_info["mFxidSys"]))

# def test_delete_node(gcp_globals):
#     print("use version to delete", gcp_globals["version"], type(gcp_globals["version"]))
#     payload = {
#         "op": "delete_node",
#         "path": gcp_globals["node_path"],
#         "session_id": "fa3a0cf0", # is defined in the client library
#         "timestamp": "fa3a0cf0-2",
#         "version": gcp_globals["version"]
#     }
#     data = json.dumps(payload).encode()

#     future = gcp_globals["writer_queue"].publisher_client.publish(gcp_globals["writer_queue"].topic_path, data= data, ordering_key= "fa3a0cf0")
#     future.result()

#     # since we dont have gcp as a client doing event scheduling in the client library, we sleep here
#     time.sleep(10)
    
#     # check newly create node
#     node_info = gcp_globals["state_storage"].read(gcp_globals["node_path"])
#     assert node_info.key.name == gcp_globals["node_path"]
#     assert "cFxidSys" not in node_info
