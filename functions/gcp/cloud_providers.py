# later we will move the enum to faaskeeper client repo config.py
from enum import IntEnum

class CLOUD_PROVIDER(IntEnum):
    AWS = 0
    GCP = 1
    AZURE = 2
