from typing import Union

import boto3

from faaskeeper.stats import StorageStatistics

from .storage import Storage


class S3Storage(Storage):
    def __init__(self, bucket_name: str):
        super().__init__(bucket_name)
        self._s3 = boto3.client("s3")

    def write(self, key: str, data: Union[dict, bytes]):
        self._s3.put_object(Body=data, Bucket=self.storage_name, Key=key)
        StorageStatistics.instance().add_write_units(1)

    def update(self, key: str, data: dict):
        """S3 update"""
        # FIXME
        pass

    def read(self, key: str):
        obj = self._s3.get_object(Bucket=self.storage_name, Key=key)
        StorageStatistics.instance().add_read_units(1)
        return obj["Body"].read()

    def delete(self, key: str):
        self._s3.delete_object(Bucket=self.storage_name, Key=key)
        StorageStatistics.instance().add_write_units(1)

    @property
    def errorSupplier(self):
        """S3 exceptions"""
        return self._s3.exceptions
