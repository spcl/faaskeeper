from .storage import Storage
# from google.cloud import firestore_admin_v1
from google.cloud import datastore
import google.cloud.exceptions

# Note: It is the Native FireStore, not the fireStore in DataStore Mode 
# This class is a wrapper containing gcp firestore python API
class DataStoreStorage(Storage):
    def __init__(self, kind_name: str) -> None:
        # collection_name should be put in yaml file
        # note the key name (primary key: path, we know path is the key, but it is not the key name) is accessed by 
        # datastore Key class: KEY.name
        super().__init__(kind_name)
        # SETUP authentication
        # and db name
        # put in config?
        self.client = datastore.Client()
        assert self.client is not None
        
    @property
    def errorSupplier(self): # we can directly use gcp
        return google.cloud.exceptions

    # functions below are supplied when using fireStore(KV store) as a User Storage.
    def write(self):
        # creating an Entity
        # kind is like a table name: {service}-{stage}-state
        # identifier is path
        # upsert
        pass
        

    def update(self):
        pass

    def read(self, path: str):
        # a read w/o using TXN
        # access the key in the return value using node_info.key
        assert self.client is not None
        node_key = self.client.key(self.storage_name, path)
        node_info = self.client.get(node_key)

        return node_info

    def delete(self):
        pass

    def _toSchema(self):
        pass

    # TODO: find a way to merge it with existing serialization code
    def update_node(self):
        pass