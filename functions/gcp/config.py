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
    PUBSUB = 1
    # DYNAMODB = 0
    # SQS = 1


class ChannelType(Enum):
    TCP = 0
    # SQS = 1


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
            # s3_data_bucket = environ["S3_DATA_BUCKET"]
            cloud_storage_bucket = environ["CLOUD_STORAGE_BUCKET"]
            self._user_storage = model.CloudStorageStorage(bucket_name=cloud_storage_bucket)
        else:
            raise RuntimeError("Not implemented!")
            # self._user_storage = model.UserDynamoStorage(
            #     table_name=f"{self._deployment_name}-data"
            # )

        # configure system storage handle
        self._system_storage_type = {"key-value": Storage.KEY_VALUE}.get(
            environ["SYSTEM_STORAGE"]
        )
        if self._system_storage_type == Storage.KEY_VALUE:
            self._system_storage = model.DataStoreSystemStateStorage(f"{self._deployment_name}")
        else:
            raise RuntimeError("Not implemented!")

        # configure distributor queue
        self._distributor_queue: Optional[distributor_queue.DistributorQueue]
        if with_distributor_queue:
            self._distributor_queue_type = {
                # "dynamodb": QueueType.DYNAMODB,
                "pubsub": QueueType.PUBSUB,
            }.get(environ["DISTRIBUTOR_QUEUE"])
            # if self._distributor_queue_type == QueueType.DYNAMODB:
            #     self._distributor_queue = distributor_queue.DistributorQueueDynamo(
            #         f"{self._deployment_name}"
            #     )
            if self._distributor_queue_type == QueueType.PUBSUB:
                self._distributor_queue = distributor_queue.DistributorQueuePubSub( # FIXME: pass project and topic id
                    "top-cascade-392319", "faasPubSub"
                )
            else:
                raise RuntimeError("Not implemented!")
        else:
            self._distributor_queue = None

        # configure client channel
        self._client_channel_type = {
            "tcp": ChannelType.TCP,
            # "sqs": ChannelType.SQS,
        }.get(environ["CLIENT_CHANNEL"])

        self._client_channel: control.ClientChannel
        if self._client_channel_type == ChannelType.TCP:
            self._client_channel = control.ClientChannelTCP()
        # elif self._client_channel_type == ChannelType.SQS:
        #     self._client_channel = control.ClientChannelSQS()
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
