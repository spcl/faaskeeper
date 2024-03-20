import logging
from enum import Enum
from os import environ
from typing import Optional

import functions.gcp.control as control
import functions.gcp.model as model
from functions.gcp.control import distributor_queue

class Storage(Enum):
    PERSISTENT = 0
    KEY_VALUE = 1
    REDIS = 2

class QueueType(Enum):
    PUBSUB = 0

class ChannelType(Enum):
    TCP = 0


class Config:

    _instance: Optional["Config"] = None

    def __init__(self, with_distributor_queue: bool = True):
        self._verbose = bool(environ["VERBOSE"])
        self._deployment_name = f"faaskeeper-{environ['DEPLOYMENT_NAME']}"
        self._deployment_region = environ["GCP_REGION"] # us-central1

        logging_format = "%(asctime)s,%(msecs)d %(levelname)s %(name)s: %(message)s"
        logging_date_format = "%H:%M:%S"
        logging.basicConfig(
            format=logging_format,
            datefmt=logging_date_format,
            level=logging.INFO if self._verbose else logging.WARNING,
            # force=True,
        )

        # configure user storage handle
        self._user_storage_type = {
            "persistent": Storage.PERSISTENT,
            "key-value": Storage.KEY_VALUE,
        }.get(environ["USER_STORAGE"])
        self._user_storage: model.UserStorage
        if self._user_storage_type == Storage.PERSISTENT:
            bucket_name = environ["CLOUD_STORAGE_BUCKET"]
            self._user_storage = model.CloudStorageStorage(bucket_name=bucket_name)
        else:
            raise RuntimeError("Not implemented!")
        # configure system storage handle
        self._system_storage_type = {"key-value": Storage.KEY_VALUE}.get(
            environ["SYSTEM_STORAGE"]
        )
        if self._system_storage_type == Storage.KEY_VALUE:
            self._system_storage = model.DataStoreSystemStateStorage(environ["PROJECT_ID"], f"{self._deployment_name}", environ["DB_NAME"])
        else:
            raise RuntimeError("Not implemented!")

        # configure distributor queue
        self._distributor_queue: Optional[distributor_queue.DistributorQueue]
        if with_distributor_queue:
            self._distributor_queue_type = {
                "pubsub": QueueType.PUBSUB,
            }.get(environ["DISTRIBUTOR_QUEUE"])
            if self._distributor_queue_type == QueueType.PUBSUB:
                self._distributor_queue = distributor_queue.DistributorQueuePubSub(
                    environ["PROJECT_ID"], environ["DISTRIBUTOR_QUEUE_NAME"]
                )
            else:
                raise RuntimeError("Not implemented!")
        else:
            self._distributor_queue = None

        # configure client channel
        self._client_channel_type = {
            "tcp": ChannelType.TCP,
        }.get(environ["CLIENT_CHANNEL"])

        self._client_channel: control.ClientChannel
        if self._client_channel_type == ChannelType.TCP:
            self._client_channel = control.ClientChannelTCP()
        else:
            raise RuntimeError("Not implemented!")

        self._benchmarking = False
        if "BENCHMARKING" in environ:
            self._benchmarking = environ["BENCHMARKING"].lower() == "true"
        self._benchmarking_frequency = 1
        if "BENCHMARKING_FREQUENCY" in environ:
            self._benchmarking_frequency = int(environ["BENCHMARKING_FREQUENCY"])

    @staticmethod
    def instance(with_distributor_queue: bool = True) -> "Config":
        if not Config._instance:
            Config._instance = Config(with_distributor_queue)
        return Config._instance

    @property
    def verbose(self) -> bool:
        return self._verbose

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def deployment_region(self) -> str:
        return self._deployment_region

    @property
    def user_storage(self) -> model.UserStorage:
        return self._user_storage

    @property
    def system_storage(self) -> model.SystemStorage:
        return self._system_storage

    @property
    def distributor_queue(self) -> Optional[distributor_queue.DistributorQueue]:
        return self._distributor_queue

    @property
    def client_channel(self) -> control.ClientChannel:
        return self._client_channel

    @property
    def benchmarking(self) -> bool:
        return self._benchmarking

    @property
    def benchmarking_frequency(self) -> int:
        return self._benchmarking_frequency
