#!/usr/bin/env python3

import os
import sys
import traceback
from inspect import signature
from typing import List

import click

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir, 'client'))

from faaskeeper.client import FaaSKeeperClient
from faaskeeper.exceptions import FaaSKeeperException, TimeoutException, NodeExistsException

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
    "getEphemerals",
]
clientAPIMapping = {
    "create": "create",
    "get": "get_data",
    "set": "set_data",
    "close": "stop",
    "connect": "start"
}

fkCompleter = WordCompleter(keywords, ignore_case = True)

def process_cmd(client: FaaSKeeperClient, cmd: str, args: List[str]):

    # process commands not offered by the API
    if cmd in ['ls', 'logs']:
        return

    # create mapping
    function = getattr(client, clientAPIMapping[cmd])
    sig = signature(function)
    params_count = len(sig.parameters)
    # incorrect number of parameters
    if params_count != len(args):
        msg = f"{cmd} arguments:"
        for param in sig.parameters.values():
            msg += f" {param.name}:{param.annotation.__name__}"
        click.echo(msg)
        return

    # convert arguments
    converted_arguments = []
    for idx, param in enumerate(sig.parameters.values()):
        if bytes == param.annotation:
            converted_arguments.append(args[idx].encode())
        elif bool == param.annotation:
            converted_arguments.append(bool(args[idx]))
        else:
            converted_arguments.append(args[idx])
    try:
        ret = function(*converted_arguments)
        click.echo(ret)
    except NodeExistsException as e:
        click.echo(e)
    except TimeoutException as e:
        click.echo(e)
        click.echo("Closing down session.")
        client.stop()
        return "DISCONNECTED", None
    except FaaSKeeperException as e:
        click.echo("Execution of the command failed.")
        click.echo(e)
        traceback.print_exc()

    return "CONNECTED", client.session_id

@click.command()
@click.argument("provider", type=click.Choice(["aws", "gcp", "azure"]))
@click.argument("service-name", type=str)
@click.option("--port", type=int, default=-1)
def cli(provider: str, service_name: str, port: int):
    session = PromptSession(
        completer=fkCompleter,
        history=FileHistory('fk_history.txt'),
        auto_suggest=AutoSuggestFromHistory()
    )

    status = "DISCONNECTED"
    counter = 0
    session_id = None
    try:
        client = FaaSKeeperClient(provider, service_name, port)
        client.start()
        status = "CONNECTED"
        session_id = client.session_id
    #FIXME: FK exceptions
    except Exception as e:
        click.echo("Unable to connect")
        click.echo(e)


    while True:
        try:
            text = session.prompt(f"[fk: {provider}:{service_name}({status}) session:{session_id} {counter}] ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        cmds = text.split()
        if len(cmds) == 0:
           continue 

        cmd = cmds[0]
        if cmd == 'quit':
            break
        elif cmd == 'help':
            click.echo("Available commands")
            click.echo(keywords)
        elif cmd == 'logs':
            click.echo_via_pager(client.logs())
        elif cmd not in keywords:
            click.echo(f"Unknown command {text}")
        else:
            status, session_id = process_cmd(client, cmd, cmds[1:])
        counter += 1

    print("Closing...")

if __name__ == "__main__":
    cli()
