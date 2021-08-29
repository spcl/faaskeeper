import boto3

from .storage import Storage


class S3Storage(Storage):
    def __init__(self):
        self._s3 = boto3.client("s3")

    def write(self, storage_name: str, key: str, data: str):
        """S3 write"""

        self._s3.put_object(
            Body=Storage._toSchema(key, data), Bucket=storage_name, Key=key
        )

    def update(self, storage_name: str, key: str, data: dict):
        """S3 update"""

        # FIXME
        pass

    def read(self, storage_name: str, key: str):
        """S3 read"""

        obj = self._s3.get_object(Bucket=storage_name, Key=key)

        return obj.get()["Body"].read()

    def delete(self, storage_name: str, key: str):
        """S3 delete"""

        self._s3.delete_object(Bucket=storage_name, Key=key).delete()

    def errorSupplier(self):
        """S3 exceptions"""

        return self._s3.exceptions
