import logging
from enum import Enum
from os import environ
from typing import Optional

import functions.aws.control as control
import functions.aws.model as model


class Storage(Enum):
    PERSISTENT = 0
    KEY_VALUE = 1
    REDIS = 2


class QueueType(Enum):
    DYNAMODB = 0
    SQS = 1


class ChannelType(Enum):
    TCP = 0
    SQS = 1


class Config:

    _instance: Optional["Config"] = None

    def __init__(self, with_distributor_queue: bool = True):
        self._verbose = bool(environ["VERBOSE"])
        self._deployment_name = f"faaskeeper-{environ['DEPLOYMENT_NAME']}"
        self._deployment_region = environ["AWS_REGION"]

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
            s3_data_bucket = environ["S3_DATA_BUCKET"]
            self._user_storage = model.UserS3Storage(bucket_name=s3_data_bucket)
        else:
            self._user_storage = model.UserDynamoStorage(
                table_name=f"{self._deployment_name}-data"
            )

        # configure system storage handle
        self._system_storage_type = {"key-value": Storage.KEY_VALUE}.get(
            environ["SYSTEM_STORAGE"]
        )
        if self._system_storage_type == Storage.KEY_VALUE:
            self._system_storage = model.SystemDynamoStorage(f"{self._deployment_name}")
        else:
            raise RuntimeError("Not implemented!")

        # configure distributor queue
        self._distributor_queue: Optional[control.DistributorQueue]
        if with_distributor_queue:
            self._distributor_queue_type = {
                "dynamodb": QueueType.DYNAMODB,
                "sqs": QueueType.SQS,
            }.get(environ["DISTRIBUTOR_QUEUE"])
            if self._distributor_queue_type == QueueType.DYNAMODB:
                self._distributor_queue = control.DistributorQueueDynamo(
                    f"{self._deployment_name}"
                )
            elif self._distributor_queue_type == QueueType.SQS:
                self._distributor_queue = control.DistributorQueueSQS(
                    environ["QUEUE_PREFIX"], self.deployment_region
                )
            else:
                raise RuntimeError("Not implemented!")
        else:
            self._distributor_queue = None

        # configure client channel
        self._client_channel_type = {
            "tcp": ChannelType.TCP,
            "sqs": ChannelType.SQS,
        }.get(environ["CLIENT_CHANNEL"])

        self._client_channel: control.ClientChannel
        if self._client_channel_type == ChannelType.TCP:
            self._client_channel = control.ClientChannelTCP()
        elif self._client_channel_type == ChannelType.SQS:
            self._client_channel = control.ClientChannelSQS()
        else:
            raise RuntimeError("Not implemented!")

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
    def distributor_queue(self) -> Optional[control.DistributorQueue]:
        return self._distributor_queue

    @property
    def client_channel(self) -> control.ClientChannel:
        return self._client_channel
