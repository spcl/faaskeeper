#!/usr/bin/env python3

import functools
import logging
import os
import subprocess

from functions.aws.init import init as aws_init

import click

# Executing with shell provides options such as wildcard expansion
def execute(cmd, shell=False, cwd=None, env=None):
    if not shell:
        cmd = cmd.split()
    ret = subprocess.run(
        cmd, shell=shell, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    if ret.returncode:
        raise RuntimeError(
            "Running {} failed!\n Output: {}".format(cmd, ret.stdout.decode("utf-8"))
        )
    return ret.stdout.decode("utf-8")

def common_params(func):
    @click.option('--provider', type=click.Choice(['aws', 'azure', 'gcp']), required=True)
    @click.option('--verbose', type=bool, default=False)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper

@click.group()
def cli():
    logging.basicConfig(level=logging.INFO)

@cli.group(invoke_without_command=True)
@click.pass_context
def deploy(ctx):
    if ctx.invoked_subcommand is None:
        service.main()

@deploy.command()
@common_params
@click.option('--clean', type=bool, default=False)
def service(provider: str, verbose: bool, clean: bool):
    env = {
        **os.environ,
        'FK_VERBOSE': str(verbose)
    }
    if clean:
        logging.info(f"Remove existing service at provider: {provider}")
        execute(f"sls remove -c config/{provider}.yml", env=env)

    logging.info(f"Deploy service to provider: {provider}")
    execute(f"sls deploy -c config/{provider}.yml", env=env)

    #FIXME: other providers
    #FIXME: name and stage
    aws_init("faaskeeper-dev")

@deploy.command()
@common_params
def functions(provider: str, verbose: bool):
    env = {
        **os.environ,
        'FK_VERBOSE': str(verbose)
    }
    logging.info(f"Deploy functions to provider: {provider}")
    execute(f"sls deploy --function writer -c config/{provider}.yml", env=env)

if __name__ == '__main__':
    cli()
