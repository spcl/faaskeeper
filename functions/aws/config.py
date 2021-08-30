from enum import Enum
from os import environ
from typing import Optional

import functions.aws.model as model


class Storage(Enum):
    PERSISTENT = 0
    KEY_VALUE = 1


class Config:

    _instance: Optional["Config"] = None

    def __init__(self):
        self._verbose = bool(environ["VERBOSE"])
        self._deployment_name = environ["DEPLOYMENT_NAME"]

        # configure user storage handle
        self._user_storage_type = {
            "persistent": Storage.PERSISTENT,
            "key-value": Storage.KEY_VALUE,
        }.get(environ["USER_STORAGE"])
        if self._user_storage_type == Storage.PERSISTENT:
            self._user_storage = model.UserS3Storage(
                bucket_name=f"faaskeeper-{self._deployment_name}-data"
            )
        else:
            self._user_storage = model.UserDynamoStorage(
                table_name=f"faaskeeper-{self._deployment_name}-data"
            )

        # configure system storage handle
        self._system_storage_type = {"key-value": Storage.KEY_VALUE}.get(
            environ["SYSTEM_STORAGE"]
        )
        if self._system_storage_type == Storage.KEY_VALUE:
            self._system_storage = model.SystemDynamoStorage(
                f"faaskeeper-{self._deployment_name}"
            )
        else:
            raise RuntimeError("Not implemented!")

    @staticmethod
    def instance() -> "Config":
        if not Config._instance:
            Config._instance = Config()
        return Config._instance

    @property
    def verbose(self) -> bool:
        return self._verbose

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def user_storage(self) -> model.UserStorage:
        return self._user_storage

    @property
    def system_storage(self) -> model.SystemStorage:
        return self._system_storage
