import os
from typing import cast

import pytest

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.exceptions import FaaSKeeperException

SERVICE_NAME = os.environ.get("FK_TEST_SERVICE_NAME")
SERVICE_REGION = os.environ.get("FK_TEST_SERVICE_REGION")
SERVICE_PORT = int(cast(str, os.environ.get("FK_TEST_SERVICE_PORT")))


@pytest.fixture
def aws_connect():
    client = FaaSKeeperClient(
        "aws",
        service_name=SERVICE_NAME,
        region=SERVICE_REGION,
        port=SERVICE_PORT,
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
