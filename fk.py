#!/usr/bin/env python3

import functools
import json
import logging
import os
import subprocess

import click

from functions.aws.init import init as aws_init, clean as aws_clean, config as aws_config

def get_env(config: dict) -> dict:

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
        "FK_CLIENT_CHANNEL": str(config_json["client-channel"]),
        "SLS_DEBUG": "*",
    }
    if "configuration" in config_json:
        env["FK_FUNCTION_BENCHMARKING"] = str(config_json["configuration"]["benchmarking"])
        env["FK_FUNCTION_BENCHMARKING_FREQUENCY"] = str(config_json["configuration"]["benchmarking-frequency"])
    else:
        env["FK_FUNCTION_BENCHMARKING"] = "False"
        env["FK_FUNCTION_BENCHMARKING_FREQUENCY"] = "0"

    return env

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

@cli.group(invoke_without_command=True)
@click.pass_context
@common_params
def export(ctx, provider: str, config):

    config_json = json.load(config)

    service_name = config_json["deployment-name"]
    try:
        logging.info(
            f"Exporting env variables for service {service_name} at provider: {provider}"
        )
        execute(
            f"sls export-env --stage {service_name} -c {provider}.yml", env=env
        )
    except Exception as e:
        logging.error("Export env didn't succeed!")
        logging.error(e)

@deploy.command()
@click.argument("output_config")
@common_params
@click.option("--clean/--no-clean", default=False)
def service(output_config: str, provider: str, config, clean: bool):

    config_json = json.load(config)
    env = get_env(config_json)

    service_name = config_json["deployment-name"]
    if clean:
        try:
            logging.info(
                f"Remove existing service {service_name} at provider: {provider}"
            )
            if provider == "aws":
                execute(f"sls export-env --stage {service_name} -c {provider}.yml", env=env)
                aws_clean(f"faaskeeper-{service_name}", config_json["deployment-region"])
            execute(
                f"sls remove --stage {service_name} -c {provider}.yml", env=env
            )
        except Exception as e:
            logging.warning("Removing service didn't succeed!")
            logging.warning(e)

    logging.info(f"Deploy service {service_name} to provider: {provider}")
    execute(f"sls deploy --stage {service_name} -c {provider}.yml", env=env)
    execute(f"sls export-env --stage {service_name} -c {provider}.yml", env=env)

    if provider == "aws":
        aws_init(f"faaskeeper-{service_name}", config_json["deployment-region"])
        final_config = aws_config(config_json)
        logging.info(f"Exporting FaaSKeeper config to {output_config}!")
        json.dump(final_config, open(output_config, 'w'), indent=2)

@deploy.command()
@common_params
@click.option("--function", type=str, default="")
def functions(provider: str, config, function: str):

    config_json = json.load(config)
    env = get_env(config_json)

    service_name = config_json["deployment-name"]
    logging.info(f"Deploy functions to service {service_name} at provider: {provider}")

    if function:
        functions = [function]
    else:
        functions = ["writer", "distributor", "watch", "heartbeat"]
    for func in functions:
        execute(
            f"sls deploy --stage {service_name} --function {func} -c {provider}.yml",
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
    env = get_env(config_json)

    service_name = config_json["deployment-name"]
    logging.info(f"Remove existing service {service_name} at provider: {provider}")
    execute(f"sls export-env --stage {service_name} -c {provider}.yml", env=env)
    if provider == "aws":
        aws_clean(f"faaskeeper-{service_name}", config_json["deployment-region"])
    execute(f"sls remove --stage {service_name} -c {provider}.yml", env=env)


if __name__ == "__main__":
    cli()
