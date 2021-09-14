import boto3


def deinit(service_name: str, region: str):

    client = boto3.client("events", region_name=region)
    client.disable_rule(Name="heartbeat-schedule")
