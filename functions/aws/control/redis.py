import base64
from typing import Union
from rediscluster import RedisCluster
from faaskeeper.node import Node
import boto3
from .storage import Storage

# FIXME
# acquire from AWS Redis ACL resource
# make it pass as config variable
redis_port = 6379
redis_host = "TBD"
redis_username = "TBD"
redis_password = "TBD"


class RedisStorage(Storage):
    def __init__(self, cluster_name: str):
        super().__init__(cluster_name)
        self._redis = RedisCluster(startup_nodes=[{"host": redis_host, "port": redis_port}],
                                   decode_responses=True, skip_full_coverage_check=True,
                                   ssl=True, username=redis_username, password=redis_password)

    def write(self, key: str, data: Union[bytes, str]):
        """Redis write"""

        print(data)
        return self._redis.hset(key, data)

    def update(self, key: str, data: dict):
        """Redis update"""

        # FIXME
        # Same logic as in S3 update
        pass

    def read(self, key: str):
        """Redis read"""

        for mask in self._redis.scan_iter(key):
            return self._redis.hgetcall(mask)

        return None

    def delete(self, key: str):
        """Redis delete"""

        self._redis.delete(key)

    @property
    def errorSupplier(self):
        """Redis exceptions"""

        return None
