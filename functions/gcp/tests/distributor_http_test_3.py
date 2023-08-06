from unittest.mock import Mock

import functions.gcp.distributor as distributor
import json
import base64

# {
#     data: "ImRhdGF2ZXIxIg=="
#     event_id: "b39a86894c2efb1b5197db8fb8ae7d2e" # this is from AWS, should be replaced by message_id
#     lock_timestamp: 1689955248
#     parent_children: [2]
#     parent_lock_timestamp: "1689955248"
#     parent_path: "/roo4"
#     path: "/roo4/faa2"
#     session_id: "fa3a0cf0"
#     timestamp: "fa3a0cf0-5"
#     type: "0"
# }

def test_print_name():
    payload = {'session_id': 'fa3a0cf0', 'timestamp': 'fa3a0cf0-5', 'type': 0, 'event_id': '7c75aaaf0413f5ef82320e05f689cb38', 'lock_timestamp': 1690704308, 'parent_lock_timestamp': 1690704309, 'path': '/root', 'parent_path': '/', 'parent_children': ['root'], 'data': 'ImRhdGF2ZXIxIg=='}


    data = {"message": {
        'data': base64.b64encode(json.dumps(payload).encode())
    }}

    req = Mock(get_json=Mock(return_value=data), args=data) # get jason is function name

    # Call tested function
    assert distributor.handler(req) == 'OK'


# def test_print_hello_world():
#     data = {}
#     req = Mock(get_json=Mock(return_value=data), args=data)

#     # Call tested function
#     assert main.distributor(req) == "Hello World!"
