class S3(Storage):
    def write(self, path: str, data: str) -> str:
        """S3/DynamoDB write"""
        pass

    def read(self, full_file_name: str) -> str:
        """S3/DynamoDB read"""
        pass

    def delete(self, full_file_name: str) -> str:
        """S3/DynamoDB delete"""
        pass