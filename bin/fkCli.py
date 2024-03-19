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
# argument should be a string
ARGS = {
    "path": "/path:str",
    "data": "data:str",
}

# flag should be a set of its representations (strings)
KWARGS = {
    "ephemeral": {"-e", "--ephemeral"},
    "sequence": {"-s", "--sequence"},
    "boolean1": {"-b1", "--boolean1"},
    "boolean2": {"-b2", "--boolean2"},
}

# program can distinguish kwarg and arg by their type
CMD = {
    # create /test 0x0 False False
    "create": [ARGS["path"], ARGS["data"], KWARGS["ephemeral"], KWARGS["sequence"]],  
    # test /test False True 0x0
    "test": [ARGS["path"], KWARGS["boolean1"], KWARGS["boolean2"], ARGS["data"]],

    # add more commands format here for parsing...
}
# parse status code
PARSE_SUCCESS = 1
PARSE_ERROR = 0

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
            if args[idx].lower() == "true":
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


# find the position of the argument in the command (function call like), return -1 if the argument is not a flag
def kwarg_pos(arg: str, kwargs_array: List[str]):
    for idx, kwarg in enumerate(kwargs_array):
        if (type(kwarg) is set) and (arg in kwarg):
            return idx
    return -1

# print the command format and flags to user
def print_cmd_info(cmd: str):
    args = ""
    flags = ""
    for arg in CMD[cmd]:
        if type(arg) is str:
            args += f"{arg} "
        else:
            flags += f"{arg} "
    click.echo(f"Command: {args}| Flags: {flags}")


# parse the arguments and return the parsed arguments
# status code: 0 - success, 1 - not need to parse, 2 - error  
def parse_args(cmd: str, args: List[str]):
    if cmd not in CMD:
        return args, PARSE_SUCCESS
    else:
        parsed_args = [False] * len(CMD[cmd])
        arg_idx = parse_args_idx = 0
        while arg_idx < len(args):
            idx = kwarg_pos(args[arg_idx], CMD[cmd])
            if idx != -1:
                parsed_args[idx] = True
            else:
                while isinstance(CMD[cmd][parse_args_idx], set): # skip the positions for flags
                    parse_args_idx += 1
                parsed_args[parse_args_idx] = args[arg_idx]
                parse_args_idx += 1
            arg_idx += 1
    return parsed_args, PARSE_SUCCESS


@click.command()
@click.argument("config", type=click.File("r"))
@click.option("--port", type=int, default=-1)
@click.option("--verbose/--no-verbose", type=bool, default=False)
def cli(config, port: int, verbose: str):
    session = PromptSession(
        completer=fkCompleter,
        history=FileHistory("fk_history.txt"),
        auto_suggest=AutoSuggestFromHistory(),
    )

    status = "DISCONNECTED"
    counter = 0
    session_id = None
    cfg = Config.deserialize(json.load(config))
    provider = CloudProvider.serialize(cfg.cloud_provider)
    service_name = f"faaskeeper-{cfg.deployment_name}"
    try:
        client = FaaSKeeperClient(cfg, port, verbose)
        client.start()
        status = "CONNECTED"
        session_id = client.session_id
    # FIXME: FK exceptions
    except Exception as e:
        click.echo("Unable to connect")
        click.echo(e)
        import traceback
        traceback.print_exc()

    while True:
        try:
            text = session.prompt(
                f"[fk: {datetime.now()} {provider}:{service_name}({status}) "
                f"session:{session_id} {counter}] "
            )
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        cmds = text.split()
        if len(cmds) == 0:
            continue

        cmd = cmds[0]
        if cmd == "quit":
            break
        elif cmd == "help":
            click.echo("Available commands")
            click.echo(keywords)
        elif cmd not in keywords:
            click.echo(f"Unknown command {text}")
        else:
            print(cmd, cmds[1:])
            parsed_args = parse_args(cmd, cmds[1:])
            status, session_id = process_cmd(client, cmd, parsed_args)
        counter += 1

    print("Closing...")
    try:
        client.stop()
    except Exception as e:
        click.echo("Unable to close the session")
        click.echo(e)
        return

    print("Session closed correctly.")


if __name__ == "__main__":
    cli()
