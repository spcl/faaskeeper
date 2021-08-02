class Storage:
    def write(self, storage_name: str, key: str, data: str):
        """S3/DynamoDB write"""
        pass

    def read(self, storage_name: str, key: str):
        """S3/DynamoDB read"""
        pass

    def delete(self, storage_name: str, key: str):
        """S3/DynamoDB delete"""
        pass