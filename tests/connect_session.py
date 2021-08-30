import json
import os

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import Config
from faaskeeper.exceptions import FaaSKeeperException

CONFIGURATION_JSON = os.environ.get("TEST_CONFIGURATION_JSON")
CLOUD_PROVIDER = os.environ.get("TEST_CLOUD_PROVIDER")
USER_STORAGE = os.environ.get("TEST_USER_STORAGE")


@pytest.fixture
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
        yield client
        client.stop()


@pytest.mark.parametrize("client", ["aws_connect"])
def test_connection(client, request):

    client = request.getfixturevalue(client)

    try:
        client.start()
        assert client.session_id
        assert client.session_status == "CONNECTED"
    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

    try:
        client.stop()
        assert not client.session_id
        assert client.session_status == "DISCONNECTED"
    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")


@pytest.mark.parametrize("client", ["aws_connect"])
def test_reconnection(client, request):

    client = request.getfixturevalue(client)

    try:
        client.start()
        assert client.session_id
        assert client.session_status == "CONNECTED"
        old_session_id = client.session_id
    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

    try:
        client.stop()
        assert not client.session_id
        assert client.session_status == "DISCONNECTED"
    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")

    #  reconnect
    try:
        client.start()
        assert client.session_id
        # this should be a new session
        assert client.session_id != old_session_id
        assert client.session_status == "CONNECTED"

        # client.stop()
    except FaaSKeeperException as e:
        pytest.fail(f"Unexpected FaaSKeeperException exception {e}")
    except Exception as e:
        pytest.fail(f"Unexpected general exception {e}")
