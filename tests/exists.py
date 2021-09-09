import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import FaaSKeeperException, MalformedInputException

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
def test_exists(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    # verify that
    try:
        path = "/test_exists1"
        client.create(path, b"10")

        read_node = client.exists(path)
        print(read_node)
        assert read_node
        assert read_node.path == path
        # exists options doesn't return node data
        assert not read_node.has_data
        # counter values should be the same
        assert read_node.created.system == read_node.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_exists_async(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    # verify that
    try:
        path = "/test_exists2"
        client.create(path, b"10")

        f = client.exists_async(path)
        read_node = f.get()
        assert read_node
        assert read_node.path == path
        # exists options doesn't return node data
        assert not read_node.has_data
        # counter values should be the same
        assert read_node.created.system == read_node.modified.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

@pytest.mark.parametrize("client", ["aws_connect"])
def test_exists_root(client, request):

    client = request.getfixturevalue(client)

    read_node = client.exists("/")
    assert read_node is not None

@pytest.mark.parametrize("client", ["aws_connect"])
def test_exists_nonexistent(client, request):

    client = request.getfixturevalue(client)

    path = "/test_exists3"
    read_node = client.exists(path)
    assert read_node is None


@pytest.mark.parametrize("client", ["aws_connect"])
def test_exists_malformed(client, request):

    client = request.getfixturevalue(client)

    with pytest.raises(MalformedInputException):
        client.set_data("test_create", b"")

    with pytest.raises(MalformedInputException):
        client.set_data("/test_create/", b"")
