from abc import ABC, abstractmethod
from enum import Enum
from collections import namedtuple
from typing import Tuple, Optional, Set, List
from os import environ

from faaskeeper.version import SystemCounter, Version
from faaskeeper.stats import StorageStatistics
from faaskeeper.node import Node, NodeDataType

from functions.gcp.control.datastore import DataStoreStorage as DataStoreDriver

from google.cloud import datastore

# FIXME: should we move this class into a more general interface?
class NodeWithLock:
    class Status(Enum):
        EXISTS = 0
        NOT_EXISTS = 1

    Lock = namedtuple("Lock", ["timestamp"])
    
    def __init__(self, node: Node, status: Status):
        self._node = node
        self._status = status
        self._pending_updates: List[str] = []
        self._lock:Optional[NodeWithLock.Lock] = None # because a NodeWithLock should be inited w/o a lock
    
    @property
    def lock(self) -> "NodeWithLock.Lock":
        assert self._lock is not None
        return self._lock
    
    @lock.setter
    def lock(self, timestamp: int): # timestamp: str or int
        self._lock = NodeWithLock.Lock(timestamp=timestamp)
    
    @property
    def isLocked(self) -> bool:
        # locked in AWS
        return self._lock is not None

    @property
    def pending_updates(self) -> List[str]:
        assert self._pending_updates is not None
        return self._pending_updates
    
    @pending_updates.setter
    def pending_updates(self, updates: List[str]):
        self._pending_updates = updates

    @property
    def status(self) -> Status:
        return self._status

    @property
    def node(self) -> Node:
        return self._node

class SystemStateStorage(ABC):
    @property
    def lock_lifetime(self) -> int:
        '''
        Clients are allowed to hold the lock for no more than 5 seconds.
        We add 2 seconds to account for clock drift of max 1 second.
        '''
        return 7

    @abstractmethod
    def lock_node(self, path: str, timestamp: int) -> Tuple[bool, Optional[Node]]:
        pass

    @abstractmethod
    def unlock_node(self, path: str, timestamp: int) -> bool:
        # we separate commit_node from unlock node
        pass
    
    @abstractmethod
    def commit_and_unlock_node(self, node: Node, timestamp: int, updates: Set[NodeDataType] = set(), update_event_id: Optional[str] = None) -> bool:
        # self.commit_node
        pass

    @abstractmethod
    def commit_and_unlock_nodes_multi(self, updates: List[NodeWithLock], deletions: List[NodeWithLock] = [],return_old_on_failure: Optional[List[Node]] = None,) -> Tuple[bool, List[NodeWithLock]]:
          # self.commit_nodes
          # commit a list of node updates
        pass

    @abstractmethod
    def generate_commit_node(self, node: Node, timestamp: int,  updates: Set[NodeDataType] = set(), update_event_id: Optional[str] = None) -> NodeWithLock:
        pass

    # @abstractmethod
    # def delete_node(self, node: Node, timestamp: int, update_event_id: Optional[str] = None):
    #     pass

    @abstractmethod
    def generate_delete_node(self, node: Node, timestamp: int, update_event_id: Optional[str] = None):
        pass

    # @abstractmethod
    # def increase_system_counter(self, writer_func_id: int) -> Optional[SystemCounter]:
    #     pass

    @abstractmethod 
    def read_node(self, node: Node) -> NodeWithLock:
        # because the timelock param is like a temp property of node
        pass
    
    @abstractmethod
    def pop_pending_update(self, node: Node) -> None:
        pass

    @abstractmethod
    def delete_user(self, session_id: str):
        """
        Remove contents stored in the object/row in the storage.
        """
        pass


class DataStoreSystemStateStorage(SystemStateStorage):
    '''
    to achieve the same conditonal update in AWS, we use transaction to check the condition
    '''
    def __init__(self, project_id: str, table_mame_prefix: str, database: str) -> None:
        #super().__init__()
        # Kind is Table, key is primary key -> faaskeeper-dev
        # key: actual node path -> path
        self._state_storage = DataStoreDriver(project_id, kind_name=f"{table_mame_prefix}-state", database=database)
        self._user_session_storage = DataStoreDriver(project_id, kind_name=f"{table_mame_prefix}-users", database=database)
        # serializer and deserializer like dynamoDB? looks like no

    def lock_node(self, path: str, timestamp: int) -> Tuple[bool, Node]:
        # why we monve this one to control driver? like we can call self._state_storage.lock_node()?
        # because this is a model class abstract away the cloud details. FIXME: move this to the interface of control driver
        # in AWS, there is a try block catching conditioncheckfailedException, here we will do it in a TXN 
        local_client = self._state_storage.client
        assert local_client is not None
        try:
            with local_client.transaction():
                node_info = self._state_storage.read(path)
                if node_info == None:
                    # node does not exist, therefore we upsert here, same with AWS
                    # note that if a node does NOT exist,
                    # the datastore only has (path, timelock)
                    key = local_client.key(self._state_storage.storage_name, path)
                    node = datastore.Entity(key)

                    node.update({
                        "timelock": timestamp
                    })
                    
                    StorageStatistics.instance().add_write_units(1) 
                        
                    local_client.put(node)

                    return (True, None) # newly created
                else:
                    # node exist
                    # the datastore has it recorded as (path, cFxidSys, mFxidSys, children)

                    # check conditions: we lock the node if 
                    # 1. lock does not exist
                    # 2. old lock expires
                    if "timelock" not in node_info or node_info["timelock"] < (timestamp - self.lock_lifetime):
                        # check succeeds
                        # lock the node
                        node_info["timelock"] = timestamp
                        local_client.put(node_info)

                        StorageStatistics.instance().add_write_units(1)
                        
                        # construct the Node instance based on result, if the node exist because a session could be disconnected right after this step
                        n: Optional[Node] = None
                        if "cFxidSys" in node_info:
                            n = Node(path)
                            created = SystemCounter.from_raw_data(
                                node_info["cFxidSys"] 
                            )
                            n.created = Version(
                                created,
                                None
                                # EpochCounter.from_provider_schema(data["cFxidEpoch"]),
                            )
                            modified = SystemCounter.from_raw_data(
                                node_info["mFxidSys"]  # type: ignore
                            )
                            n.modified = Version(
                                modified,
                                None
                                # EpochCounter.from_provider_schema(data["mFxidEpoch"]),
                            )
                            n.children = node_info["children"]
                        return (True, n)

                return (False, None)
        except self._state_storage.errorSupplier.Conflict:
            print("lock_node |", "there is a conflict, lock node fails")
            return (False, None)
        
    def unlock_node(self, path: str, lock_timestamp: int) -> bool:
        """
        We need to make sure that we're still the ones holding a timelock.
        Then, we need to remove the timelock.

        We don't perform any additional updates - just unlock.
        
        what is committed here? just to remove lock
        the implementatio is slightly diff from AWS's for the sake of readability.
        """         
        
        local_client = self._state_storage.client
        assert local_client is not None

        try:
            with local_client.transaction():
                node_info = self._state_storage.read(path)

                if "timelock" in node_info and node_info["timelock"] == lock_timestamp:
                    del node_info["timelock"]
                    assert "timelock" not in node_info

                local_client.put(node_info)
                StorageStatistics.instance().add_write_units(1)    
                return True

        except self._state_storage.errorSupplier.Conflict:
            print("there is a conflict, lock node fails")
            return False

    def commit_and_unlock_node(self, node: Node, timestamp: int, updates: Set[NodeDataType] = set(), update_event_id: Optional[str] = None) -> bool:
        local_client = self._state_storage.client
        assert local_client is not None

        success: bool = True
        
        try:
            with local_client.transaction():
                node_info = self._state_storage.read(node.path)

                if node_info is not None:
                    to_commit = self.generate_commit_node(node, timestamp, updates, update_event_id)
                    if "timelock" in node_info and node_info["timelock"] == to_commit._lock:
                        del node_info["timelock"]
                        assert "timelock" not in to_commit.commit_details

                        if to_commit._update_event_id_to_append is not None:
                            temp = node_info["pendingUpdates"] + to_commit._update_event_id_to_append
                            to_commit.commit_details["pendingUpdates"] = temp

                        # override new properties of node and keep the unchanged ones.
                        for property_to_update in to_commit.commit_details:
                            node_info[property_to_update] = to_commit.commit_details[property_to_update]
                        local_client.put(node_info)
                        return success
        except self._state_storage.errorSupplier.Conflict:
            print("there is a conflict, lock node fails")
            success = False
            return success
        return False

    class CommitNode(NodeWithLock):
        def __init__(self, node: Node, status: NodeWithLock.Status, timelock: NodeWithLock.Lock):
            super().__init__(node, status)
            self._lock = timelock
            self._commit_details = {}
            self._update_event_id_to_append:List[str] = None
        
        @property
        def commit_details(self):
            return self._commit_details
        
        @commit_details.setter
        def commit_details(self, detail: dict):
            self._commit_details = detail

    def commit_and_unlock_nodes_multi(self, updates: List[CommitNode], deletions: List[CommitNode] = [], return_old_on_failure: List[Node] = None) -> Tuple[bool, List[NodeWithLock]]:
        
        local_client = self._state_storage.client
        assert local_client is not None

        success: bool
        old_values: List[NodeWithLock] = []

        try:
            with local_client.transaction():
                update_keys = []
                update_mapper = {} 
                for update in updates:
                    update_key = local_client.key(self._state_storage.storage_name,update.node.path)
                    update_keys.append(update_key)
                    update_mapper[update.node.path] = update

                nodes = local_client.get_multi(update_keys)
                nodes_to_update = []
                for node in nodes:
                    to_commit: DataStoreSystemStateStorage.CommitNode = update_mapper[node.key.name]

                    if "timelock" in node and node["timelock"] == to_commit._lock:
                        del node["timelock"]
                        assert "timelock" not in to_commit.commit_details

                        if to_commit._update_event_id_to_append is not None: # set data
                            temp = node["pendingUpdates"] + to_commit._update_event_id_to_append
                            to_commit.commit_details["pendingUpdates"] = temp

                        # override new properties of node and keep the unchanged ones.
                        for property_to_update in to_commit.commit_details:
                            node[property_to_update] = to_commit.commit_details[property_to_update]
                        nodes_to_update.append(node)

                local_client.put_multi(nodes_to_update)

                # deletes
                delete_keys:List[datastore.Key] = []
                delete_mapper = {}
                for delete in deletions:
                    delete_keys.append(local_client.key(self._state_storage.storage_name,delete.node.path))
                    delete_mapper[delete.node.path] = delete
                nodes_to_delete = []
                nodes = local_client.get_multi(delete_keys)
                for node in nodes:
                    to_commit: DataStoreSystemStateStorage.CommitNode = delete_mapper[node.key.name]
                    if "timelock" in node and node["timelock"] == to_commit._lock:
                        del node["timelock"]
                        del node["cFxidSys"]
                        del node["mFxidSys"]
                        del node["children"]

                        if to_commit._update_event_id_to_append is not None: # set data
                            temp = node["pendingUpdates"] + to_commit._update_event_id_to_append
                            to_commit.commit_details["pendingUpdates"] = temp
                        else:
                            # only keep pendingUpdates, remove created, modified, children, timelock
                            to_commit.commit_details["pendingUpdates"] = node["pendingUpdates"]
                        
                        node["pendingUpdates"] = to_commit.commit_details["pendingUpdates"]
                        nodes_to_delete.append(node)
                local_client.put_multi(nodes_to_delete)

                StorageStatistics.instance().add_write_units(len(update_keys) + len(delete_keys))
                
                success = True
                return (success, old_values)
            
        except self._state_storage.errorSupplier.Conflict:
            print("there is a conflict, lock node fails")
            success = False
            return (success, old_values)
    
    def generate_commit_node(self, node: Node, timestamp: int, updates: Set[NodeDataType] = set(), update_event_id: str = None) -> CommitNode:
        # Similar to AWS, we generate a dict for each node containing updated properties: cFxidSys, mFxidSys, pendingUpdates, children
        # in the following structure key: node.path values: dict {properties}
        local_client = self._state_storage.client
        assert local_client is not None

        ret = DataStoreSystemStateStorage.CommitNode(node, status=None, timelock=timestamp)

        update_values = {}
        if NodeDataType.CREATED in updates:
            update_values["cFxidSys"] = node.created.system._version
            update_values["pendingUpdates"] = [] if update_event_id is None else [update_event_id]
        
        elif update_event_id is not None: # not created and update_event_id is not None
            # still update pending updates, append to existing pendingUpdate list
            # since we can not get the list in this function, we did concat in the commit_and_unlock_nodes_multi()
            ret._update_event_id_to_append = [update_event_id]

        if NodeDataType.MODIFIED in updates:
            update_values["mFxidSys"] = node.modified.system._version
        
        if NodeDataType.CHILDREN in updates:
            update_values["children"] = node.children
            
        ret.commit_details = update_values

        return ret
    
    def read_node(self, node: Node) -> NodeWithLock:
        res = self._state_storage.read(node.path)
        StorageStatistics.instance().add_read_units(1)
        
        return self._parse_node(node, res)
    
    def _parse_node(self, node: Node, response: dict, complete_data=True) -> NodeWithLock:
        if response == None:
            return NodeWithLock(node, NodeWithLock.Status.NOT_EXISTS)
        
        dynamo_node: NodeWithLock

        if "cFxidSys" not in response:
            dynamo_node = NodeWithLock(node, NodeWithLock.Status.NOT_EXISTS)
        else:
            dynamo_node = NodeWithLock(node, NodeWithLock.Status.EXISTS)
        
        if "timelock" in response:
            dynamo_node.lock = response["timelock"]
        
        if "pendingUpdates" in response:
            dynamo_node.pending_updates = response["pendingUpdates"]
        else:
            dynamo_node.pending_updates = []
        
        if dynamo_node.status == NodeWithLock.Status.NOT_EXISTS or not complete_data:
            return dynamo_node

        if "cFxidSys" in response:
            created = SystemCounter.from_raw_data(response["cFxidSys"])  # type: ignore
            dynamo_node.node.created = Version(
                created,
                None
                # EpochCounter.from_provider_schema(data["cFxidEpoch"]),
        )
            
        if "mFxidSys" in response:
            modified = SystemCounter.from_raw_data(response["mFxidSys"])  # type: ignore
            dynamo_node.node.modified = Version(modified, None)
        
        if "children" in response:
            dynamo_node.node.children = response["children"]
        
        return dynamo_node
    
    def pop_pending_update(self, node: Node) -> None:
        local_client = self._state_storage.client
        assert local_client is not None
        try:
            with local_client.transaction():
                node_info = self._state_storage.read(node.path)
                if node_info == None:
                    return None
                else:
                    if "pendingUpdates" in node_info and len(node_info["pendingUpdates"]) > 0:
                        node_info["pendingUpdates"].pop(0)
                        local_client.put(node_info)

                        StorageStatistics.instance().add_write_units(1)
                    return None

        except self._state_storage.errorSupplier.Conflict:
            print("lock_node |", "there is a conflict, lock node fails")
            return (None)
    
    def generate_delete_node(self, node: Node, timestamp: int, update_event_id: Optional[str] = None):
        local_client = self._state_storage.client
        assert local_client is not None

        ret = DataStoreSystemStateStorage.CommitNode(node, status=None, timelock=timestamp) # we do not need Exist property here

        if update_event_id is not None:
            ret._update_event_id_to_append = [update_event_id]
            
        ret.commit_details = {}

        return ret

    def delete_user(self, session_id: str):
        try:
            self._user_session_storage.delete(session_id)
            return True
        except Exception:
            return False