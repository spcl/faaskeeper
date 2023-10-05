import os
from google.cloud import datastore
from google.cloud import storage
from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version
from faaskeeper.providers.serialization import S3Reader

def init(service_name: str, region: str):
    # service_name: faaskeeper-{service_name_in_fk}
    cloud_storage_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")
    assert cloud_storage_bucket is not None
    
    # clean up system state table
    datastore_client = datastore.Client()
    kind_name = f"{service_name}-state"
    # # initialize root
    with datastore_client.transaction():
        root_node = datastore.Entity(datastore_client.key(kind_name, "/"))
        root_node.update({
            "cFxidSys": [0],
            "mFxidSys": [0],
            "children": []
        })

        datastore_client.put(root_node)

    # clean up user state table
    cloud_storage_client = storage.Client()
    bucket_name = "faaskeeper1"
    cloud_storage_bucket = cloud_storage_client.bucket(bucket_name)
    # # initialize root

    root_node = Node("/")
    root_node.created = Version(SystemCounter.from_raw_data([int("0")]), None)
    root_node.modified = Version(
        SystemCounter.from_raw_data([0]), EpochCounter.from_raw_data(set())
    )
    root_node.children = []
    root_node.data = b""

    root_blob = cloud_storage_bucket.blob("/")
    root_blob.upload_from_string(data=S3Reader.serialize(root_node))