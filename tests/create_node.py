import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import (
    FaaSKeeperException,
    MalformedInputException,
    NodeDoesntExistException,
    NodeExistsException,
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


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        node = client.create("/test_create", b"")

        assert node
        assert node.path == "/test_create"

        read_node = client.get_data("/test_create")
        assert read_node
        assert read_node.path == "/test_create"
        assert read_node.data == b""
        # creeate returned correct timestamp
        assert node.created.system == read_node.created.system
        # created timestamp same as modified
        assert read_node.created.system == read_node.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_async(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        f = client.create_async("/test_create2", b"1")
        node = f.get()

        assert node
        assert node.path == "/test_create2"

        read_node = client.get_data("/test_create2")
        assert read_node
        assert read_node.path == "/test_create2"
        assert read_node.data == b"1"
        assert read_node.created.system == read_node.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_malformed(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(MalformedInputException):
        client.create("test_create", b"")

    with pytest.raises(MalformedInputException):
        client.create("/test_create/", b"")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_repeated(client, request):

    client = request.getfixturevalue(client)
    client.create("/test_create3", b"")

    with pytest.raises(NodeExistsException):
        client.create("/test_create3", b"")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_incorrect_parent(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(NodeDoesntExistException):
        client.create("/test_create6/test_create", b"")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_correct_parent(client, request):

    client = request.getfixturevalue(client)

    client.create("/test_create5", b"")
    client.create("/test_create5/test_create", b"12")

    read_node = client.get_data("/test_create5/test_create")
    assert read_node
    assert read_node.path == "/test_create5/test_create"
    assert read_node.data == b"12"


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_repeated_async(client, request):

    """
        Verify that exception is propagated through future correctly.
    """

    client = request.getfixturevalue(client)
    client.create("/test_create4", b"")

    with pytest.raises(NodeExistsException):
        f = client.create_async("/test_create4", b"")
        f.get()
