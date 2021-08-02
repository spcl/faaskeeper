import boto3
import json

class S3(Storage):
    def __init__(self):
        s3 = boto3.client("s3")

    def write(self, storage_name: str, key: str, data: str) -> str:
        """S3/DynamoDB write"""
        s3.put_object(Body=data, Bucket=storage_name, Key=key)
        pass

    def read(self, storage_name: str, key: str) -> str:
        """S3/DynamoDB read"""
        obj = s3.get_object(Bucket=storage_name, Key=key)
        return obj.get()["Body"].read()

    def delete(self, storage_name: str, key: str) -> str:
        """S3/DynamoDB delete"""
        s3.delete_object(Bucket=storage_name, Key=key).delete()