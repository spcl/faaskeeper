#!/usr/bin/env python3

import logging
import click

@click.group()
def cli():
    logging.basicConfig(level=logging.INFO)

@cli.group()
@click.option('--provider', type=click.Choice(['aws', 'azure', 'gcp']), required=True)
@click.pass_context
def deploy(ctx, provider):

    ctx.ensure_object(dict)
    ctx.obj['provider'] = provider

@deploy.command()
@click.pass_context
def functions(ctx):
    logging.info('Deploy to ' + ctx.obj['provider'])

@deploy.command()
@click.pass_context
def (ctx):
    logging.info('Kill service at ' + ctx.obj['provider'])

@cli.group()
@click.option('--provider', type=click.Choice(['aws', 'azure', 'gcp']), required=True)
@click.option('--example', type=str, required=True)
@click.pass_context
def examples(ctx, provider, example):

    ctx.ensure_object(dict)
    ctx.obj['provider'] = provider
    ctx.obj['example'] = example

@examples.command()
@click.pass_context
def invoke(ctx):
    logging.info('Deploy to ' + ctx.obj['provider'])

if __name__ == '__main__':
    cli()