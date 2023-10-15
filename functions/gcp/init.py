import os
from google.cloud import datastore
from google.cloud import storage
from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version
from faaskeeper.providers.serialization import S3Reader

def init(service_name: str, region: str):
    # service_name: faaskeeper-{service_name_in_fk}
    # deployment_name: {service_name_in_fk}
    bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
    deployment_name = os.environ.get("DEPLOYMENT_NAME")
    cloud_storage_bucket = f"sls-gcp-{deployment_name}-{bucket_name}"
    assert cloud_storage_bucket is not None
    
    # clean up system state table
    datastore_client = datastore.Client(project=os.environ.get("PROJECT_ID"),database="test2")
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
    cloud_storage_bucket = cloud_storage_client.bucket(cloud_storage_bucket)
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