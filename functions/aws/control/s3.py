from typing import Union

import boto3

from .storage import Storage


class S3Storage(Storage):
    def __init__(self, bucket_name: str):
        super().__init__(bucket_name)
        self._s3 = boto3.client("s3")

    def write(self, key: str, data: Union[dict, bytes]):
        self._s3.put_object(Body=data, Bucket=self.storage_name, Key=key)

    def update(self, key: str, data: dict):
        """S3 update"""
        # FIXME
        pass

    def read(self, key: str):
        print(key)
        obj = self._s3.get_object(Bucket=self.storage_name, Key=key)
        return obj["Body"].read()

    def delete(self, key: str):
        """S3 delete"""
        self._s3.delete_object(Bucket=self.storage_name, Key=key).delete()

    @property
    def errorSupplier(self):
        """S3 exceptions"""
        return self._s3.exceptions
