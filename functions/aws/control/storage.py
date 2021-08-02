class Storage:
    def __init__(self):
        super().__init__()

    def write(self, storage_name: str, key: str, data: str):
        pass

    def read(self, storage_name: str, key: str):
        pass

    def delete(self, storage_name: str, key: str):
        pass

    def errorSupplier(self):
        pass
