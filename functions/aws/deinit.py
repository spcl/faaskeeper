import boto3

import functions.aws.model as model
from faaskeeper.node import Node
from faaskeeper.version import SystemCounter, Version


def deinit(service_name: str, region: str):

    client = boto3.client('events', region_name=region)
    client.disable_rule(Name="heartbeat-schedule")
