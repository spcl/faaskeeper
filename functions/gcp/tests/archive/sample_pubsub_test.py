import base64

from unittest import mock
import pytest

from . import sample_cloud_func
# import sample_cloud_func
import json

mock_context = mock.Mock()
mock_context.event_id = "617187464135194"
mock_context.timestamp = "2019-07-15T22:09:03.761Z"
mock_context.resource = {
    "name": "projects/my-project/topics/my-topic",
    "service": "pubsub.googleapis.com",
    "type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
}

test_data = {
    "session_id": "fa3a0cf0",
    "timestamp": "fa3a0cf0-5",
    "type": "0",
    "event_id": "b39a86894c2efb1b5197db8fb8ae7d2e",
    "lock_timestamp": 1689955248,
    "parent_lock_timestamp": "1689955248",
    "path": "/roo4/faa2",
    "parent_path": "/roo4",
    "parent_children": ["faas1", "faa2"],
    "data": str(base64.b64decode("ImRhdGF2ZXIxIg==")) #base64.b64decode
}


def test_print_hello_world(capsys):
    data = {}

    # Call tested function
    sample_cloud_func.hello_pubsub(data, mock_context)
    out, err = capsys.readouterr()
    assert "Hello World!" in out

def test_print_name(capsys):
    name = json.dumps(test_data)
    mock_event = {"data": base64.b64encode(name.encode())}

    # Call tested function, use json.dump, take results from AWS and modify them accordingly.
    sample_cloud_func.hello_pubsub(mock_event, mock_context)
    out, err = capsys.readouterr()
    print(out)
    assert f"Hello {name}!\n" in out
