import json
from typing import Union

import redis

from faaskeeper.stats import StorageStatistics

from .storage import Storage

# FIXME: configurable
REDIS_HOST = "44.199.217.133"


class RedisStorage(Storage):
    def __init__(self):
        # FIXME: prper naming
        super().__init__("redis")
        self._redis = redis.Redis(host=REDIS_HOST, port=6379, db=0)

    def write(self, key: str, data: Union[dict, bytes]):
        self._redis.set(key, data)
        # StorageStatistics.instance().add_write_units(1)

    def update(self, key: str, data: dict):
        """S3 update"""
        # FIXME
        pass

    def read(self, key: str):
        return self._redis.get(key)

    def delete(self, key: str):
        self._redis.delete(key)

    @property
    def errorSupplier(self):
        """S3 exceptions"""
        return self._s3.exceptions
