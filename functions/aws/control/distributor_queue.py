import json
from abc import ABC, abstractmethod
from typing import Dict

import boto3
from boto3.dynamodb.types import TypeSerializer

from faaskeeper.version import SystemCounter
from functions.aws.control.channel import Client
from functions.aws.control.dynamo import DynamoStorage as DynamoDriver

from .distributor_events import DistributorEvent


class DistributorQueue(ABC):
    @abstractmethod
    def push(self, counter: SystemCounter, event: DistributorEvent, client: Client):
        pass


class DistributorQueueDynamo(DistributorQueue):
    def __init__(self, deployment_name: str):
        self._queue = DynamoDriver(f"{deployment_name}-distribute-queue", "key")
        self._type_serializer = TypeSerializer()

    # FIXME: remove from here ip, port
    def push(self, counter: SystemCounter, event: DistributorEvent, client: Client):
        """
            We must use a single shard - everything is serialized.
        """
        counter_val = counter.sum

        # when launching from a Dynamo trigger, the binary value
        # is not automatically base64 decoded
        # however, we can't put base64 encoded data to boto3:
        # it ALWAYS applies encoding,
        # regardless of the format of data
        # https://github.com/boto/boto3/issues/3291
        # https://github.com/aws/aws-cli/issues/1097
        #
        # thus, we need to decode the value first

        client_serialization = {
            x: self._type_serializer.serialize(y) for x, y in client.serialize().items()
        }
        self._queue.write(
            "",
            {
                "key": self._type_serializer.serialize("faaskeeper"),
                "system_counter": self._type_serializer.serialize(counter_val),
                **client_serialization,
                **event.serialize(self._type_serializer, base64_encoded=False),
            },
        )


class DistributorQueueSQS(DistributorQueue):
    def __init__(self, name: str, region: str):

        self._sqs_client = boto3.client("sqs", region)
        response = self._sqs_client.get_queue_url(
            QueueName=f"{name}-distributor-sqs.fifo"
        )
        self._sqs_queue_url = response["QueueUrl"]

        self._type_serializer = TypeSerializer()

    def push(self, counter: SystemCounter, event: DistributorEvent, client: Client):

        # FIXME: is it safe here to serialize the types?
        client_serialization = {
            x: self._type_serializer.serialize(y) for x, y in client.serialize().items()
        }
        payload: Dict[str, str] = {
            **client_serialization,  # type: ignore
            **event.serialize(self._type_serializer),
        }

        attributes: dict = {}
        self._sqs_client.send_message(
            QueueUrl=self._sqs_queue_url,
            MessageBody=json.dumps(payload),
            MessageAttributes=attributes,
            MessageGroupId="0",
            MessageDeduplicationId=str(counter.sum),
        )
