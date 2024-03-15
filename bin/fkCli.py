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
CMD_KWARGS = {
    "create": [{"-e", "--ephemeral"}, {"-s", "--sequence"}],
    # add more commands format here for parsing
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


def parse_args(cmd: str, args: List[str]):
    if cmd not in CMD_KWARGS:
        return args
    else:
        parsed_args = []
        parsed_kwargs = [False] * len(CMD_KWARGS[cmd])
        for arg in args:
            is_kwarg = False
            for idx, kwarg in enumerate(CMD_KWARGS[cmd]):
                if arg in kwarg:
                    parsed_kwargs[idx] = True
                    is_kwarg = True
                    break
            if not is_kwarg:
                parsed_args.append(arg)
        parsed_args.extend(parsed_kwargs)
        return parsed_args


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
            args = parse_args(cmd, cmds[1:])
            status, session_id = process_cmd(client, cmd, args)
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
