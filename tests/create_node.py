import os
from typing import cast

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.exceptions import (
    FaaSKeeperException,
    MalformedInputException,
    NodeExistsException,
)

SERVICE_NAME = os.environ.get("FK_TEST_SERVICE_NAME")
SERVICE_REGION = os.environ.get("FK_TEST_SERVICE_REGION")
SERVICE_PORT = int(cast(str, os.environ.get("FK_TEST_SERVICE_PORT")))


@pytest.fixture(scope='session')
def aws_connect():
    client = FaaSKeeperClient(
        "aws",
        service_name=SERVICE_NAME,
        region=SERVICE_REGION,
        port=SERVICE_PORT,
        verbose=False,
    )
    client.start()
    return client


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        client.start()
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
        client.start()
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
