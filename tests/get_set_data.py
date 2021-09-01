import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import FaaSKeeperException, NodeDoesntExistException, MalformedInputException

CONFIGURATION_JSON = os.environ.get("TEST_CONFIGURATION_JSON")
CLOUD_PROVIDER = os.environ.get("TEST_CLOUD_PROVIDER")
USER_STORAGE = os.environ.get("TEST_USER_STORAGE")

# FIXME: add tests depending on version

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
def test_set_data(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    # verify that
    try:
        path = "/test_set_data"
        node = client.create(path, b"10")
        assert node
        assert node.path == path

        read_node = client.get_data(path)
        assert read_node
        assert read_node.path == path
        assert read_node.data == b"10"
        # counter values should be the same
        assert read_node.created.system == read_node.modified.system

        written_node = client.set_data(path, b"12")
        assert written_node
        assert written_node.path == path
        # verify that counter value is increased
        assert read_node.modified.system < written_node.modified.system

        read_node2 = client.get_data(path)
        assert read_node2
        assert read_node2.path == path
        # data is updated
        assert read_node2.data == b"12"
        # created timestamp doesn't change
        assert read_node.created.system == read_node2.created.system
        # modified timestamp is the same
        assert read_node.modified.system < read_node2.modified.system
        assert written_node.modified.system == read_node2.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_set_data_async(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    # verify that
    try:
        path = "/test_set_data2"
        node = client.create(path, b"10")
        assert node
        assert node.path == path

        f = client.get_data_async(path)
        read_node = f.get()
        assert read_node
        assert read_node.path == path
        assert read_node.data == b"10"
        # counter values should be the same
        assert read_node.created.system == read_node.modified.system

        f = client.set_data_async(path, b"12")
        written_node = f.get()
        assert written_node
        assert written_node.path == path
        # verify that counter value is increased
        assert read_node.modified.system < written_node.modified.system

        f = client.get_data_async(path)
        read_node2 = f.get()
        assert read_node2
        assert read_node2.path == path
        # data is updated
        assert read_node2.data == b"12"
        # created timestamp doesn't change
        assert read_node.created.system == read_node2.created.system
        # modified timestamp is the same
        assert read_node.modified.system < read_node2.modified.system
        assert written_node.modified.system == read_node2.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_set_data_nonexistent(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(NodeDoesntExistException):
        client.set_data("/test_set_data_nonexisten", b"")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_data_nonexistent(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(NodeDoesntExistException):
        client.get_data("/test_set_data_nonexisten")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_set_data_malformed(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(MalformedInputException):
        client.set_data("test_create", b"")

    with pytest.raises(MalformedInputException):
        client.set_data("/test_create/", b"")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_data_malformed(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(MalformedInputException):
        client.get_data("test_create")

    with pytest.raises(MalformedInputException):
        client.get_data("/test_create/")
