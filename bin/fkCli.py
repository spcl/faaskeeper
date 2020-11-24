#!/usr/bin/env python3

import os
import sys

import click

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir, 'client'))

from faaskeeper.client import FaaSKeeperClient

keywords = [
    "help",
    "connect",
    "close",
    "logs",
    "create",
    "get",
    "delete",
    "ls",
    "set",
    "stat",
    "close",
    "getEphemerals",
    "quit"
]

fkCompleter = WordCompleter(keywords, ignore_case = True)

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

    counter = 0
    client = FaaSKeeperClient(provider, service_name, port)
    client.start()
    status = "CONNECTED"

    while True:
        try:
            text = session.prompt(f"[fk: {provider}:{service_name}(status) {counter}] ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        if text == 'quit':
            break
        elif text == 'help':
            click.echo("Available commands")
            click.echo(keywords)
        elif text == 'logs':
            # FIXME: query logs
            pass
        elif text not in keywords:
            click.echo(f"Unknown command {text}")
        counter += 1

    print("Closing...")

if __name__ == "__main__":
    cli()
