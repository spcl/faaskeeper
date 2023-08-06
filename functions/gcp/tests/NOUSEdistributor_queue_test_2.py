from functions.gcp.control.distributor_queue import DistributorQueuePubSub
from faaskeeper import node
from faaskeeper.version import SystemCounter, Version
from typing import Optional

import pathlib
# NO USE
def main():
    print("Running tests... push_and_count in distributor_queue.py")

    '''
    ================================ TESTS CASES ================================
    '''
    def mock():
        print("Running mock()")
        cc = DistributorQueuePubSub("GIVEN", "GIVEN")
        print(cc.push_and_count(None, None))
    '''
    ================================ TESTS ================================
    '''
    # lock_and_unlock() # passed
    # mock()
    mock()



if __name__ == "__main__":
    main()