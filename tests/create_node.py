import json
import os
from typing import cast

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import (
    FaaSKeeperException,
    MalformedInputException,
    NodeExistsException,
)

CONFIGURATION_JSON = os.environ.get("TEST_CONFIGURATION_JSON")
CLOUD_PROVIDER = os.environ.get("TEST_CLOUD_PROVIDER")
USER_STORAGE = os.environ.get("TEST_USER_STORAGE")


@pytest.fixture(scope="module")
def aws_connect():
    with open(CONFIGURATION_JSON, 'r') as config_file:
        config = json.load(config_file)
        client = FaaSKeeperClient(
            Config.deserialize({
                **config,
                'cloud-provider': CLOUD_PROVIDER,
                'user-storage': USER_STORAGE
            }),
            port=config['port'],
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

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_async(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        f = client.create_async("/test_create2", b"")
        node = f.get()

        assert node
        assert node.path == "/test_create2"
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
def test_create_node_repeated_async(client, request):

    """
        Verify that exception is propagated through future correctly.
    """

    client = request.getfixturevalue(client)
    client.create("/test_create4", b"")

    with pytest.raises(NodeExistsException):
        f = client.create_async("/test_create4", b"")
        f.get()

