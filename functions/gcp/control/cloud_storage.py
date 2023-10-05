from .storage import Storage
from google.cloud import storage, exceptions
from typing import Union
from faaskeeper.stats import StorageStatistics

class CloudStorage(Storage):
    def __init__(self, bucket_name: str):
        super().__init__(bucket_name)
        self._storage_client = storage.Client()
        self._bucket = self._storage_client.bucket(bucket_name)
    
    def write(self, key: str, data: Union[dict, bytes]):
        blob = self._bucket.blob(key)
        blob.upload_from_string(data=data)
        StorageStatistics.instance().add_write_units(1)

    def delete(self, key: str):
        '''
        WARNING: Object deletion cannot be undone. 
        '''
        blob = self._bucket.blob(key)

        blob.reload()
        generation_match_precondition = blob.generation

        blob.delete(if_generation_match=generation_match_precondition)

        StorageStatistics.instance().add_write_units(1)

    def read(self, key: str):
        blob = self._bucket.get_blob(key)
        file_content = blob.download_as_bytes()
        StorageStatistics.instance().add_read_units(1) # 1 entity read
        return file_content
    

    def update(self, key: str, data: dict):
        # upload_from_string, so just like in AWS, we can read and write to update content 
        pass 

    @property
    def errorSupplier(self):
        return exceptions