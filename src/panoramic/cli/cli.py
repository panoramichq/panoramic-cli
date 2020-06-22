from typing import Any, Dict

import click

from panoramic.cli.scan import Scanner
from panoramic.cli.write import write


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('-i', '--source-id', type=str, help='ID of source')
@click.pass_context
def cli(ctx: click.Context, source_id: str):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['source_id'] = source_id


@cli.command(help='Scan models from source')
@click.argument('scope')
@click.pass_context
def scan(ctx: click.Context, scope: str):
    tables = Scanner(ctx.obj['source_id']).run(scope)
    write(tables)
