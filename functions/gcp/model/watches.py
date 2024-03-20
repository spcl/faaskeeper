from typing import List

from faaskeeper.watch import WatchType
from functions.gcp.control.datastore import DataStoreStorage

from collections import namedtuple

class Watches:

    Watch_Event = namedtuple("Watch_Event", ["watch_event_type", "watch_type", "node_path", "mFxidSys"])

    def __init__(self, project_id: str, database: str,storage_name: str, region: str):
        self._storage = DataStoreStorage(project_id, f"{storage_name}-watch", database)
        self._region = region
        self._counters = {
            WatchType.GET_DATA: "getData",
            WatchType.EXISTS: "createNode",
            WatchType.GET_CHILDREN: "getChildrenID",
        }

    def query_watches(self, node_path: str, counters: List[WatchType]):
        try:
            ret, _ = self._storage.read(node_path)

            data = []
            if ret != None:
                for c in counters:
                    if self._counters.get(c) in ret:
                        data.append(
                            (
                                c,
                                node_path,
                                ret[self._counters.get(c)],
                            )
                        )
            return data
        except Exception:
            return []

    def get_watches(self, node_path: str, counters: List[WatchType]):
        local_client = self._storage.client
        try:
            with local_client.transaction():
                node_info = self._storage.read(node_path)
                old_info = node_info

                for c in counters:
                    watch_type_str = self._counters.get(c)
                    if watch_type_str in old_info:
                        del old_info[watch_type_str]
                
                local_client.put(old_info)

                data = []
                if old_info != None:
                    for c in counters:
                        data.append(
                            (
                                c,
                                old_info[self._counters.get(c)],  # type: ignore
                            )
                        )
                return data
        except Exception:
            return []
