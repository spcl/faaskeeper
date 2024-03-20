import pytest

from bin.fkCli import parse_args

def test_cli():
    print("test create")
    assert ['/test', '0x0', True, True] == parse_args("create", ["-e", "-s", "/test", "0x0"])[1]
    assert ['/test', '0x0', True, True] == parse_args("create", ["/test", "0x0", "-e", "-s"])[1]
    assert ['/test', '0x0', False, False] == parse_args("create", ["/test", "0x0"])[1]
    assert ['/test', '0x0', True, False] == parse_args("create", ["/test", "0x0", "-e"])[1]
    assert ['/test', '0x0', False, True] == parse_args("create", ["/test", "0x0", "-s"])[1]
    print("test get")
    assert ['/test', True] == parse_args("get", ["/test", "-w"])[1]
    assert ['/test', True] == parse_args("get", ["-w", "/test"])[1]
    assert ['/test', False] == parse_args("get", ["/test"])[1]
    print("test getChildren")
    assert ['/test', True] == parse_args("getChildren", ["/test", "-i"])[1]
    assert ['/test', True] == parse_args("getChildren", ["-i", "/test"])[1]
    assert ['/test', False] == parse_args("getChildren", ["/test"])[1]