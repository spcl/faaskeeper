from abc import ABC, abstractmethod
from faaskeeper.version import SystemCounter
from functions.gcp.cloud_providers import CLOUD_PROVIDER
from functions.gcp.control.channel import Client
from functions.gcp.control.distributor_events import DistributorEvent
from functions.gcp.control.channel import Client

from datetime import datetime
from typing import Dict
import json
import base64

from google.cloud import pubsub_v1

class DistributorQueue(ABC):
    @abstractmethod
    def push(self, counter: SystemCounter, event: DistributorEvent, client: Client) -> None:
        pass

    @abstractmethod
    def push_and_count(self, event: DistributorEvent, client: Client) -> SystemCounter:
        pass

class DistributorQueuePubSub(DistributorQueue):
    def __init__(self,  project_id: str, topic_id: str) -> None:
        '''
        project_id is not necessary to set if you have GOOGLE_APPLICATION_CREDENTIALS configured
        '''

        # the batch will be sent if any of the setting is met.
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=10,  # default 100, now it is 10
            max_bytes= 1 * 1000 * 1000,  # default 1 MB, still 1 MB -> 1000 * 1000 KB
            max_latency=0.01,  # default 10 ms, now is 10ms
        )

        publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True) # enable FIFO
        self.publisher_client = pubsub_v1.PublisherClient(publisher_options=publisher_options, batch_settings= batch_settings)
        self._topic_id = topic_id
        self._project_id = project_id
        self._topic_path = self.publisher_client.topic_path(self._project_id, self._topic_id)

    @property
    def topic_path(self):
        return self._topic_path

    def push(self, counter: SystemCounter, event: DistributorEvent, client: Client) -> None:
        client_serialization = client.serialize()
        payload: Dict[str, str] = {
            **client_serialization,
            **event.serialize(None, CLOUD_PROVIDER.GCP)
        }

        data = base64.b64encode(json.dumps(payload).encode())
        # how does AWS do? give the same ordering key?
        future = self.publisher_client.publish(self.topic_path, data=data, ordering_key= "0")
        try:
            print(future.result()) # a successful publish response
        except RuntimeError:
            self.publisher_client.resume_publish(self.topic_path, ordering_key= "0")
    
    def push_and_count(self, event: DistributorEvent, client: Client) -> SystemCounter:
        client_serialization = client.serialize()
        payload: Dict[str, str] = {
            **client_serialization,
            **event.serialize(None, CLOUD_PROVIDER.GCP),
        }
        data = json.dumps(payload).encode() 

        future = self.publisher_client.publish(self.topic_path, data= data, ordering_key= client.session_id)
        try:
            return None
        except RuntimeError:
            self.publisher_client.resume_publish(self.topic_path, ordering_key= client.session_id)
    