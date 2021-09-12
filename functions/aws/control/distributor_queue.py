from abc import ABC, abstractmethod

from boto3.dynamodb.types import TypeSerializer

from faaskeeper.version import SystemCounter
from functions.aws.control.dynamo import DynamoStorage as DynamoDriver

from .distributor_events import DistributorEvent


class DistributorQueue(ABC):
    @abstractmethod
    def push(self, counter: SystemCounter, event: DistributorEvent):
        pass


class DistributorQueueDynamo(DistributorQueue):
    def __init__(self, deployment_name: str):
        self._queue = DynamoDriver(f"{deployment_name}-distribute-queue", "key")
        self._type_serializer = TypeSerializer()

    def push(self, counter: SystemCounter, event: DistributorEvent):
        # FIXME: update interface
        """We must use a single shard - everything is serialized.
        """
        counter_val = counter.sum
        print(
            {
                "key": self._type_serializer.serialize("faaskeeper"),
                "timestamp": self._type_serializer.serialize(counter_val),
                **event.serialize(self._type_serializer),
            },
        )
        self._queue.write(
            "",
            {
                "key": self._type_serializer.serialize("faaskeeper"),
                "timestamp": self._type_serializer.serialize(counter_val),
                **event.serialize(self._type_serializer),
            },
        )
