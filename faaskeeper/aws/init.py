
import boto3

def init(service_name: str):

    dynamodb = boto3.client("dynamodb")
    # clean state table
    dynamodb.put_item(
        TableName=f"{service_name}-state",
        Item= {
            "type": {
                "S": "zxid"
            },
            "value": {
                "N": "0"
            }
        }
    )

