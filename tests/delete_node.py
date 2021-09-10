import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import (
    FaaSKeeperException,
    NodeDoesntExistException,
    NotEmptyException,
)

CONFIGURATION_JSON = os.environ.get("TEST_CONFIGURATION_JSON")
CLOUD_PROVIDER = os.environ.get("TEST_CLOUD_PROVIDER")
USER_STORAGE = os.environ.get("TEST_USER_STORAGE")


@pytest.fixture(scope="module")
def aws_connect():
    with open(CONFIGURATION_JSON, "r") as config_file:
        config = json.load(config_file)
        client = FaaSKeeperClient(
            Config.deserialize(
                {
                    **config,
                    "cloud-provider": CLOUD_PROVIDER,
                    "user-storage": USER_STORAGE,
                }
            ),
            port=config["port"],
            verbose=False,
        )
        client.start()
        yield client
        client.stop()


"""
    Test deleting nodes and verifying that the paren'ts path is updated.
"""


@pytest.mark.parametrize("client", ["aws_connect"])
def test_delete_node(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_delete_node"
        node = client.create(path, b"")
        assert node
        assert node.path == path
        root_node = client.get_data("/")
        assert root_node
        assert "test_delete_node" in root_node.children

        # do actual delete
        client.delete(path)
        # node shouldn't exist anymore
        with pytest.raises(NodeDoesntExistException):
            node = client.get_data(path)
        # node shouldn't be on parent's list
        root_node = client.get_data("/")
        assert root_node
        assert "test_delete_node" not in root_node.children

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_delete_node_async(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_delete_node2"
        node = client.create(path, b"")
        assert node
        assert node.path == path
        root_node = client.get_data("/")
        assert root_node
        assert "test_delete_node2" in root_node.children

        # do actual delete
        f = client.delete_async(path)
        f.get()
        # node shouldn't exist anymore
        with pytest.raises(NodeDoesntExistException):
            node = client.get_data(path)
        # node shouldn't be on parent's list
        root_node = client.get_data("/")
        assert root_node
        assert "test_delete_node2" not in root_node.children

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_delete_node_incorrect(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(NodeDoesntExistException):
        client.delete("/test_delete_node3")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_delete_node_nonempty(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_delete_node4"
        client.create(path, b"")
        path = "/test_delete_node4/test_delete_node5"
        client.create(path, b"")

        # now we shouldn't be able to delete
        with pytest.raises(NotEmptyException):
            client.delete("/test_delete_node4")

        # this should work
        client.delete("/test_delete_node4/test_delete_node5")
        client.delete("/test_delete_node4")

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")
