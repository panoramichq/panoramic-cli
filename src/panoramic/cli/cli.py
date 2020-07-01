import logging

import click

from panoramic.cli.__version__ import __version__


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
def cli(debug):
    if debug:
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

    from panoramic.cli.supported_version import is_version_supported

    if not is_version_supported(__version__):
        exit(1)


@cli.command(help='Scan models from source')
@click.argument('source-id', type=str, required=True)
@click.option('--schema-filter', '-f', type=str, help='Filter down what schemas to scan')
def scan(source_id: str, schema_filter: str):
    from panoramic.cli.scan import Scanner
    from panoramic.cli.write import write

    tables = Scanner(source_id).run(schema_filter)
    write(tables)
