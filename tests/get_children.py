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


@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_children_empty(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_get_children1"
        node = client.create(path, b"")
        assert node
        assert node.path == path

        children = client.get_children(path)
        assert len(children) == 0

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_children_single(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_get_children2"
        node = client.create(path, b"")
        assert node
        assert node.path == path

        path2 = "x"
        path2_full = os.path.join(path, path2)
        node2 = client.create(path2_full, b"test_value")
        assert node2
        assert node2.path == path2_full

        children = client.get_children(path)
        assert len(children) == 1
        first_child = children[0]
        assert first_child.path == path2_full
        assert not first_child.has_data
        assert first_child.created.system == node2.created.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_children_multiple(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_get_children3"
        node = client.create(path, b"")
        assert node
        assert node.path == path

        children = []
        for child in ["x", "y", "z"]:
            path2_full = os.path.join(path, child)
            children.append(client.create(path2_full, child.encode()))

        children_nodes = client.get_children(path)
        assert len(children_nodes) == len(children)
        for i in range(len(children)):
            child = children[i]
            child_found = children_nodes[i]

            assert child_found.path == child.path
            assert not child_found.has_data
            assert child_found.created.system == child.created.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_get_children_with_data(client, request):

    client = request.getfixturevalue(client)

    # should succeed with no errors
    try:
        path = "/test_get_children4"
        node = client.create(path, b"")
        assert node
        assert node.path == path

        children = []
        children_data = []
        for child in ["x", "y", "z"]:
            path2_full = os.path.join(path, child)
            children.append(client.create(path2_full, child.encode()))
            children_data.append(child.encode())

        children_nodes = client.get_children(path, include_data=True)
        assert len(children_nodes) == len(children)
        for i in range(len(children)):
            child = children[i]
            child_data = children_data[i]
            child_found = children_nodes[i]

            assert child_found.path == child.path
            assert child_found.has_data
            assert child_found.data == child_data
            assert child_found.created.system == child.created.system

    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")
