#!/usr/bin/env python3

import json
import traceback
from datetime import datetime
from inspect import signature
from typing import List

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.config import CloudProvider, Config
from faaskeeper.exceptions import (
    BadVersionError,
    FaaSKeeperException,
    MalformedInputException,
    NodeDoesntExistException,
    NodeExistsException,
    TimeoutException,
)
from faaskeeper.watch import WatchedEvent

keywords = [
    "help",
    "logs",
    "quit",
    "connect",
    "close",
    "create",
    "get",
    "delete",
    "ls",
    "set",
    "stat",
    "exists",
    "getEphemerals",
    "getChildren",
]
clientAPIMapping = {
    "create": "create",
    "delete": "delete",
    "get": "get_data",
    "getChildren": "get_children",
    "set": "set_data",
    "exists": "exists",
    "close": "stop",
    "connect": "start",
}

fkCompleter = WordCompleter(keywords, ignore_case=True)


def watch_callback(watch_event: WatchedEvent):
    click.echo(
        f"WatchedEvent type: {watch_event.event_type} path: {watch_event.path}"
    )


def process_cmd(client: FaaSKeeperClient, cmd: str, args: List[str]):

    # process commands not offered by the API
    if cmd in ["ls", "logs"]:
        if cmd == "logs":
            click.echo_via_pager(client.logs())
        return client.session_status, client.session_id

    # create mapping
    function = getattr(client, clientAPIMapping[cmd])
    sig = signature(function)
    params_count = len(sig.parameters)
    # incorrect number of parameters
    if params_count != len(args):
        msg = f"{cmd} arguments:"
        for param in sig.parameters.values():
            # "watch" requires conversion - API uses a callback
            # the CLI is a boolean switch if callback should be use or not
            if param.name == "watch":
                msg += f" watch:bool"
            else:
                msg += f" {param.name}:{param.annotation.__name__}"
        click.echo(msg)
        return client.session_status, client.session_id

    # convert arguments
    converted_arguments = []
    for idx, param in enumerate(sig.parameters.values()):
        # "watch" requires conversion - API uses a callback
        # the CLI is a boolean switch if callback should be use or not
        if param.name == "watch":
            if bool(args[idx]):
                converted_arguments.append(watch_callback)
            else:
                converted_arguments.append(None)
            continue

        if bytes == param.annotation:
            converted_arguments.append(args[idx].encode())
        elif bool == param.annotation:
            converted_arguments.append(bool(args[idx]))
        else:
            converted_arguments.append(args[idx])
    try:
        ret = function(*converted_arguments)

        # special output handling
        if cmd == "exists" and ret is None:
            print(f"Node {args[0]} does not exist")
        elif isinstance(ret, list):
            for node in ret:
                click.echo(json.dumps(node.serialize()))
        elif ret is not None:
            click.echo(json.dumps(ret.serialize()))
    except (
        NodeExistsException,
        NodeDoesntExistException,
        BadVersionError,
        MalformedInputException,
    ) as e:
        click.echo(e)
    except TimeoutException as e:
        click.echo(e)
        click.echo("Closing down session.")
        try:
            client.stop()
        except TimeoutException:
            click.echo("Couldn't properly disconnect session.")
        return client.session_status, client.session_id
    except FaaSKeeperException as e:
        click.echo("Execution of the command failed.")
        click.echo(e)
        traceback.print_exc()

    return client.session_status, client.session_id


def create_node(**kwargs):
    print("create_node called with arguments:", kwargs)

def get_data(**kwargs):
    print("get_data called with arguments:", kwargs)

def set_data(**kwargs):
    print("set_data called with arguments:", kwargs)

@click.command()
@click.option("--command", required=True, type=click.Choice(["create", "get_data", "set_data"]))
@click.option("--name", required=False)
@click.option("--data", required=False)
@click.option("--ip", required=False)
@click.option("--port", required=False)
def process_command(command, name=None, data=None, ip=None, port=None):
    if command == "create":
        create_node(name=name, data=data)
    elif command == "get_data":
        get_data(name=name)
    elif command == "set_data":
        set_data(name=name, data=data)
    elif command == "connect":
        print("connect called with arguments:", ip, port)
    else:
        click.echo(f"Invalid command: {command}")



if __name__ == "__main__":
    cli()
