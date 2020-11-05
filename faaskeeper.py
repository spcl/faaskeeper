#!/usr/bin/env python3

import click
import functools
import logging
import subprocess

# Executing with shell provides options such as wildcard expansion
def execute(cmd, shell=False, cwd=None):
    if not shell:
        cmd = cmd.split()
    ret = subprocess.run(
        cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    if ret.returncode:
        raise RuntimeError(
            "Running {} failed!\n Output: {}".format(cmd, ret.stdout.decode("utf-8"))
        )
    return ret.stdout.decode("utf-8")

def common_params(func):
    @click.option('--provider', type=click.Choice(['aws', 'azure', 'gcp']), required=True)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper

@click.group()
def cli():
    logging.basicConfig(level=logging.INFO)

@cli.command()
@common_params
@click.option('--clean', type=bool, default=False)
def deploy(provider: str, clean: bool):
    if clean:
        logging.info(f"Remove existing service at provider: {provider}")
        execute(f"sls remove -c config/{provider}.yml")

    logging.info(f"Deploy service to provider: {provider}")
    execute(f"sls deploy -c config/{provider}.yml")

if __name__ == '__main__':
    cli()
