import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import FaaSKeeperException

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
    Test creating trees of nodes and verify that `children` value is correctly
    updated.
"""


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_nested(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_create_nested"
        node = client.create(path, b"")
        assert node
        assert node.path == path

        read_node = client.get_data(path)
        assert len(read_node.children) == 0

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_double_nested(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_create_nested2"
        client.create(path, b"")
        path2 = "test_create_nested3"
        path2_full = os.path.join(path, path2)
        client.create(path2_full, b"")

        read_node = client.get_data(path)
        assert len(read_node.children) == 1
        assert read_node.children[0] == path2

        read_node_child = client.get_data(path2_full)
        assert len(read_node_child.children) == 0

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_multiple_children(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_create_nested3"
        client.create(path, b"")
        for child in ["x", "y", "z"]:
            path2_full = os.path.join(path, child)
            client.create(path2_full, b"")

        read_node = client.get_data(path)
        assert len(read_node.children) == 3

        for child in ["x", "y", "z"]:
            assert child in read_node.children
            # check child
            path2_full = os.path.join(path, child)
            read_node_child = client.get_data(path2_full)
            assert len(read_node_child.children) == 0

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_create_node_triple_nested(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path1 = "/test_create_nested4"
        client.create(path1, b"")

        path2 = "test_create_nested5"
        path2_full = os.path.join(path1, path2)
        client.create(path2_full, b"")

        path3 = "test_create_nested6"
        path3_full = os.path.join(path1, path2, path3)
        client.create(path3_full, b"")

        read_node = client.get_data(path1)
        assert len(read_node.children) == 1
        assert read_node.children[0] == path2

        read_node = client.get_data(path2_full)
        assert len(read_node.children) == 1
        assert read_node.children[0] == path3

        read_node = client.get_data(path3_full)
        assert len(read_node.children) == 0

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")
