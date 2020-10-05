import logging
import sys
from typing import Optional

import click
from click.core import Command, Context
from dotenv import load_dotenv

from panoramic.cli.__version__ import __version__
from panoramic.cli.errors import (
    CompanyNotFoundException,
    SourceNotFoundException,
    ValidationError,
    handle_exception,
    handle_interrupt,
)
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_error, echo_errors


class ConfigAwareCommand(Command):
    """Perform config file validation before running command."""

    def invoke(self, ctx: Context):
        from panoramic.cli.validate import validate_config

        try:
            validate_config()
            return super().invoke(ctx)
        except ValidationError as e:
            echo_error(str(e))
            sys.exit(1)


class ContextAwareCommand(ConfigAwareCommand):
    """
    Perform config and context file validation before running command.

    Failure scenarios we handle:
      * Invalid config/context/local state - ValidationError
      * Company not found for user - CompanyNotFoundException
      * Source not found for company - SourceNotFoundException

    Any other error - we show stack trace.
    """

    def invoke(self, ctx: Context):
        from panoramic.cli.validate import validate_context

        try:
            validate_context()
            return super().invoke(ctx)
        except (ValidationError, SourceNotFoundException, CompanyNotFoundException) as e:
            echo_error(str(e))
            sys.exit(1)


class LocalStateAwareCommand(ContextAwareCommand):
    """Perform config, context, and local state files validation before running command."""

    def invoke(self, ctx: Context):
        from panoramic.cli.validate import validate_local_state

        errors = validate_local_state()
        if len(errors) > 0:
            echo_errors(errors)
            sys.exit(1)

        return super().invoke(ctx)


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
@handle_exception
def cli(debug):
    """Run checks at the beginning of every command."""
    if debug:
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

    load_dotenv(dotenv_path=Paths.dotenv_file())

    from panoramic.cli.supported_version import is_version_supported

    if not is_version_supported(__version__):
        sys.exit(1)


@cli.command(help='Scan models from source', cls=ContextAwareCommand)
@click.argument('source-id', type=str, required=True)
@click.option('--filter', '-f', type=str, help='Filter down what schemas to scan')
@click.option('--generate-identifiers', '-i', is_flag=True, help='Generate identifiers for models')
@click.option('--parallel', '-p', type=int, default=8, help='Parallelize metadata scan')
@handle_exception
@handle_interrupt
def scan(source_id: str, filter: Optional[str], parallel: int, generate_identifiers: bool):
    from panoramic.cli.command import scan as scan_command

    scan_command(source_id, filter, parallel, generate_identifiers)


@cli.command(help='Pull models from remote', cls=LocalStateAwareCommand)
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@click.option('--target-dataset', '-t', type=str, help='Target a specific dataset')
@click.option('--diff', '-d', is_flag=True, help='Show the difference between local and remote state')
@handle_exception
def pull(yes: bool, target_dataset: str, diff: bool):
    from panoramic.cli.command import pull as pull_command

    pull_command(yes=yes, target_dataset=target_dataset, diff=diff)


@cli.command(help='Push models to remote', cls=LocalStateAwareCommand)
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@click.option('--target-dataset', '-t', type=str, help='Target a specific dataset')
@click.option('--diff', '-d', is_flag=True, help='Show the difference between local and remote state')
@handle_exception
def push(yes: bool, target_dataset: str, diff: bool):
    from panoramic.cli.command import push as push_command

    push_command(yes=yes, target_dataset=target_dataset, diff=diff)


@cli.command(help='Configure pano CLI options')
@handle_exception
def configure():
    from panoramic.cli.command import configure as config_command

    config_command()


@cli.command(help='Initialize metadata repository', cls=ConfigAwareCommand)
@handle_exception
def init():
    from panoramic.cli.command import initialize

    initialize()


@cli.command(help='List available data connections', cls=ContextAwareCommand)
@handle_exception
def list_connections():
    from panoramic.cli.command import list_connections as list_connections_command

    list_connections_command()


@cli.command(help='List available companies', cls=ConfigAwareCommand)
@handle_exception
def list_companies():
    from panoramic.cli.command import list_companies as list_companies_command

    list_companies_command()


@cli.command(help='Validate local files')
@handle_exception
def validate():
    from panoramic.cli.command import validate as validate_command

    if not validate_command():
        sys.exit(1)


@cli.command(help='Detect joins under a dataset', cls=LocalStateAwareCommand)
@click.option('--target-dataset', '-t', type=str, help='Target a specific dataset')
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@click.option('--diff', '-d', is_flag=True, help='Show the difference between local and detected joins')
@click.option(
    '--overwrite', is_flag=True, default=False, help='Overwrite joins on local model files by suggestions from remote'
)
@handle_exception
def detect_joins(target_dataset: str, yes: bool, diff: bool, overwrite: bool):
    from panoramic.cli.command import detect_joins as detect_joins_command

    if not detect_joins_command(target_dataset=target_dataset, diff=diff, overwrite=overwrite, yes=yes):
        sys.exit(1)
