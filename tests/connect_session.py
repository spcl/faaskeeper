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
    return FaaSKeeperClient(
        "aws",
        service_name=SERVICE_NAME,
        region=SERVICE_REGION,
        port=SERVICE_PORT,
        verbose=False,
    )


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


#  reconnect after
