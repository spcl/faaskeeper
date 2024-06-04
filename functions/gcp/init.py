import os
from google.cloud import datastore
from google.cloud import storage
from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version
from faaskeeper.providers.serialization import S3Reader
from typing import Optional

def init(service_name: str, region: str, bucket_name: Optional[str],
         deployment_name: Optional[str], project_id: Optional[str], database: Optional[str]):
    # clean up system state table
    datastore_client = datastore.Client(project=project_id,database=database)
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

        # verify the result using the following url format: 
        # https://console.cloud.google.com/datastore/databases/{DB_NAME}/entities;kind={KIND_NAME}/query/kind?project={PROJECT_ID}

    # clean up user state table
    cloud_storage_client = storage.Client()
    cloud_storage_bucket = f"sls-gcp-{deployment_name}-{bucket_name}"
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