class Storage:
    def _toSchema(self, key: str, data: str):
        return {
            "path": {"S": key},
            "data": {"B": data},
            "dFxid": {"N": "0"},
            "cFxid": {"N": "0"},
            "mFxid": {"N": "0"},
            "ephemeralOwner": {"S": ""},
        }

    def write(self, storage_name: str, key: str, data: str):
        pass

    def update(self, storage_name: str, key: str, data: dict):
        pass

    def read(self, storage_name: str, key: str):
        pass

    def delete(self, storage_name: str, key: str):
        pass

    def errorSupplier(self):
        pass
