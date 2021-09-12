#!/usr/bin/env python3

import functools
import json
import logging
import os
import subprocess

import click

from functions.aws.init import init as aws_init


# Executing with shell provides options such as wildcard expansion
def execute(cmd, shell=False, cwd=None, env=None):
    if not shell:
        cmd = cmd.split()
    ret = subprocess.run(
        cmd,
        shell=shell,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if ret.returncode:
        raise RuntimeError(
            "Running {} failed!\n Output: {}".format(cmd, ret.stdout.decode("utf-8"))
        )
    return ret.stdout.decode("utf-8")


def common_params(func):
    @click.option(
        "--provider", type=click.Choice(["aws", "azure", "gcp"]), required=True
    )
    @click.option("--config", type=click.File("r"), required=True)
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
@click.option("--clean/--no-clean", default=False)
def service(provider: str, config, clean: bool):

    config_json = json.load(config)
    env = {
        **os.environ,
        "FK_VERBOSE": str(config_json["verbose"]),
        "FK_DEPLOYMENT_NAME": str(config_json["deployment-name"]),
        "FK_DEPLOYMENT_REGION": str(config_json["deployment-region"]),
        "FK_USER_STORAGE": str(config_json["user-storage"]),
        "FK_SYSTEM_STORAGE": str(config_json["system-storage"]),
        "FK_HEARTBEAT_FREQUENCY": str(config_json["heartbeat-frequency"]),
        "FK_WORKER_QUEUE": str(config_json["worker-queue"]),
        "FK_DISTRIBUTOR_QUEUE": str(config_json["distributor-queue"]),
        "SLS_DEBUG": "*",
    }
    service_name = config_json["deployment-name"]
    if clean:
        try:
            logging.info(
                f"Remove existing service {service_name} at provider: {provider}"
            )
            execute(
                f"sls remove --stage {service_name} -c config/{provider}.yml", env=env
            )
        except Exception:
            logging.warn("Removing service didn't succeed!")

    logging.info(f"Deploy service {service_name} to provider: {provider}")
    execute(f"sls deploy --stage {service_name} -c config/{provider}.yml", env=env)

    if provider == "aws":
        aws_init(f"faaskeeper-{service_name}", config_json["deployment-region"])


@deploy.command()
@common_params
@click.option("--function", type=str, default="")
def functions(provider: str, config, function: str):

    config_json = json.load(config)
    env = {
        **os.environ,
        "FK_VERBOSE": str(config_json["verbose"]),
        "FK_DEPLOYMENT_NAME": str(config_json["deployment-name"]),
        "FK_DEPLOYMENT_REGION": str(config_json["deployment-region"]),
        "FK_USER_STORAGE": str(config_json["user-storage"]),
        "FK_SYSTEM_STORAGE": str(config_json["system-storage"]),
        "FK_HEARTBEAT_FREQUENCY": str(config_json["heartbeat-frequency"]),
        "FK_WORKER_QUEUE": str(config_json["worker-queue"]),
        "FK_DISTRIBUTOR_QUEUE": str(config_json["distributor-queue"]),
    }
    service_name = config_json["deployment-name"]
    logging.info(f"Deploy functions to service {service_name} at provider: {provider}")

    if function:
        functions = [function]
    else:
        functions = ["writer", "distributor"]
    for func in functions:
        execute(
            f"sls deploy --stage {service_name} --function {func} -c config/{provider}.yml",
            env=env,
        )


@cli.group(invoke_without_command=True)
@click.pass_context
def remove(ctx):
    if ctx.invoked_subcommand is None:
        service.main()


@remove.command(name="service")
@common_params
def remove_service(provider: str, config):

    config_json = json.load(config)
    env = {
        **os.environ,
        "FK_VERBOSE": str(config_json["verbose"]),
        "FK_DEPLOYMENT_NAME": str(config_json["deployment-name"]),
        "FK_DEPLOYMENT_REGION": str(config_json["deployment-region"]),
        "FK_USER_STORAGE": str(config_json["user-storage"]),
        "FK_SYSTEM_STORAGE": str(config_json["system-storage"]),
        "FK_HEARTBEAT_FREQUENCY": str(config_json["heartbeat-frequency"]),
        "FK_WORKER_QUEUE": str(config_json["worker-queue"]),
        "FK_DISTRIBUTOR_QUEUE": str(config_json["distributor-queue"]),
    }
    service_name = config_json["deployment-name"]
    logging.info(f"Remove existing service {service_name} at provider: {provider}")
    execute(f"sls remove --stage {service_name} -c config/{provider}.yml", env=env)


if __name__ == "__main__":
    cli()
