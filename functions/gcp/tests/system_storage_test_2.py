from functions.gcp.model.system_storage import DataStoreSystemStateStorage
from faaskeeper import node
from faaskeeper.version import SystemCounter, Version
from typing import Optional

import pathlib

def main():
    print("Running tests... execute_operation(op_exec, client) in writer.py")
    # init
    test_storage = DataStoreSystemStateStorage("faaskeeper-dev")
    path = "/root"
    timestamp = 12
    _node = node.Node(path)
    _node.children = []
    node_path = pathlib.Path(path)
    _parent_path = node_path.parent.absolute()
    _parent_path = str(_parent_path)
    _parent_timestamp: Optional[int] = 11
    
    '''
    ================================ TESTS CASES ================================
    '''
    def mock():
        print("Running mock()")

    def lock_and_unlock():
        print("Running lock_and_unlock()")
        # in lock_and_read, execute_operation, writer.py
        print(test_storage.lock_node(path, timestamp))
        _, _parent_node = test_storage.lock_node(_parent_path, _parent_timestamp)
        print(_parent_node)
        if _parent_node is None:
            print(f"failure, {_parent_path} does not exist")
            test_storage.unlock_node(_parent_path, _parent_timestamp)
            test_storage.unlock_node(path, timestamp)

        print(test_storage.unlock_node(path, timestamp))
        print(test_storage.unlock_node(_parent_path, _parent_timestamp))

    def lock_commit_unlock_multi():
        print("Running lock_commit_unlock_multi()")
        print(test_storage.lock_node(path, timestamp))
        _, _parent_node = test_storage.lock_node(_parent_path, _parent_timestamp)
        print(_, _parent_node)
        # in commit_and_unlock, execute_operation, writer.py
        # try:

        _node.created = Version(SystemCounter.from_raw_data([1]), None)
        _node.modified = Version(SystemCounter.from_raw_data([1]), None)

        print(test_storage.commit_and_unlock_nodes_multi(updates=[
            test_storage.generate_commit_node(
                _node,
                timestamp,
                set([
                    node.NodeDataType.CREATED,
                    node.NodeDataType.MODIFIED,
                    node.NodeDataType.CHILDREN,
                ]),
                1 #self.event_id
            ),
            # figure out levels of parent
            test_storage.generate_commit_node(
                _parent_node,
                _parent_timestamp,
                set([node.NodeDataType.CHILDREN]),
            ),
        ]))
        # except BaseException as e:
        #     print('An exception occurred: {}'.format(e))
        #     print(test_storage.unlock_node(path, timestamp))
        #     print(test_storage.unlock_node(_parent_path, _parent_timestamp))
    '''
    ================================ TESTS ================================
    '''
    # lock_and_unlock() # passed
    # mock()
    lock_commit_unlock_multi()

    
    # import pathlib
    # path = "/root/test01/rear"
    # node_path = pathlib.Path(path)
    # parent_path = node_path.parent.absolute() #  {"/root". par: "/"}, {"/root/re", par: "/root"}
    # print(parent_path)


if __name__ == "__main__":
    main()