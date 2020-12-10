import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional, Tuple

import click
from click.core import Command, Context
from dbt.main import main as dbt_main
from dotenv import load_dotenv

from panoramic.cli.__version__ import __version__
from panoramic.cli.analytics import is_enabled, write_command_event
from panoramic.cli.command import configure_anonymous_analytics
from panoramic.cli.errors import (
    CompanyNotFoundException,
    SourceNotFoundException,
    ValidationError,
    ValidationErrorSeverity,
    handle_exception,
    handle_interrupt,
)
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_error, echo_errors, echo_info, echo_warnings
from panoramic.cli.validate import validate_dbt

_PROFILES_DIR_ARG = '--profiles-dir'
_PROJECT_DIR_ARG = '--project-dir'


class CommandWithAnalytics(Command):
    """Execute command and write usage metrics to a file that is conditionally flushed to external system."""

    def invoke(self, ctx: Context):
        # This will make sure to display opt-out message for existing users that already run pano configure.
        if not is_enabled():
            configure_anonymous_analytics()

        # If command is a subcommand, combine its name with the command name.
        group = ''
        if ctx.parent is not None and ctx.parent.command != cli:  # type: ignore
            group = ctx.parent.command.name  # type: ignore

        start_time = time.time()
        try:
            result = super().invoke(ctx)
            write_command_event(self.name, group, start_time)
            return result
        except Exception as e:
            write_command_event(self.name, group, start_time, error=str(e))
            raise e


class ConfigAwareCommand(CommandWithAnalytics):
    """Perform config file validation before running command."""

    def invoke(self, ctx: Context):
        from panoramic.cli.validate import validate_config

        cwd = Path.cwd()

        try:
            validate_config()
            return super().invoke(ctx)
        except ValidationError as e:
            echo_error(str(e))
            sys.exit(1)
        finally:
            os.chdir(cwd)  # DBT sets cwd so we need to reset it


class ConnectionAwareCommand(ConfigAwareCommand):
    def invoke(self, ctx: Context):
        try:
            return super().invoke(ctx)
        except Exception as e:
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


class DbtCommand(ContextAwareCommand):
    """Command proxied to DBT CLI tool."""

    def invoke(self, ctx: Context):
        from panoramic.cli.dbt import prepare_dbt_project

        try:
            validate_dbt()
            prepare_dbt_project()
            return super().invoke(ctx)
        except Exception as e:
            # TODO: Catch DBT exception here?
            echo_error(str(e))
            sys.exit(1)


class LocalStateAwareCommand(ContextAwareCommand):
    """Perform config, context, and local state files validation before running command."""

    def invoke(self, ctx: Context):
        from panoramic.cli.validate import validate_local_state

        errors_by_severity: defaultdict = defaultdict(list)
        for error in validate_local_state():
            errors_by_severity[error.severity].append(error)

        if len(errors_by_severity[ValidationErrorSeverity.WARNING]) > 0:
            echo_warnings(errors_by_severity[ValidationErrorSeverity.WARNING])
            echo_info('')

        if len(errors_by_severity[ValidationErrorSeverity.ERROR]) > 0:
            echo_errors(errors_by_severity[ValidationErrorSeverity.ERROR])
            sys.exit(1)

        return super().invoke(ctx)


@click.group(context_settings={'help_option_names': ["-h", "--help"]}, help='')
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


@cli.command(help='Validate local files', cls=CommandWithAnalytics)
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

    detect_joins_command(target_dataset=target_dataset, diff=diff, overwrite=overwrite, yes=yes)


@cli.group(name='field')
def field_cli():
    """Commands on local field files."""


@field_cli.command(help='Clean up fields not linked to any model', cls=ContextAwareCommand)
@click.option('--target-dataset', '-t', type=str, help='Target a specific dataset')
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@handle_exception
def cleanup(target_dataset: str, yes: bool):
    from panoramic.cli.command import delete_orphaned_fields as delete_orphans_command

    delete_orphans_command(target_dataset=target_dataset, yes=yes)


@field_cli.command(help='Scaffold fields defined in models', cls=ContextAwareCommand)
@click.option('--target-dataset', '-t', type=str, help='Target a specific dataset')
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@handle_exception
def scaffold(target_dataset: str, yes: bool):
    from panoramic.cli.command import (
        scaffold_missing_fields as scaffold_missing_fields_command,
    )

    scaffold_missing_fields_command(target_dataset=target_dataset, yes=yes)


@cli.group()
def connection():
    """Connection subcommand for managing connections.

    \b
    All connections are stored in ~/.pano/config file.
    You can edit this file either manually or using provided commands.

    Expected YAML structure:

    \b
    name:
      type: postgres
      user: my_user
      password: <password>
      host: localhost
      port: 5432
      database: my_db

    Which effectively results in a connection string similar to: "postgres://my_user@<password>@localhost:5432/my_db".

    \b
    name: The name of the connection that will be used as reference to specify which connection to use.
    type: The type of data warehouse you are connecting to.
    user: The username used for database connection.
    password: The password used for database connection.
    host: The hostname (server) used for database connection.
    port: The port used for database connection.
    database: The database used for database connection.
    schema: The schema to build models into by default. Can be overridden by custom models.
    warehouse: The warehouse to use when building models. (Used by Snowflake)
    account: The account used for database connection. This will be something like cc123 or cc123.us-east-1 for your particular account. (Used by Snowflake)
    project: The project used for database connection. (Used by BigQuery)
    key_file: Keyfile path to Service Account JSON. (Used by BigQuery)
    """
    pass


@connection.command(cls=ConnectionAwareCommand)
@click.argument('name', type=str)
@click.option('--type', default=None, type=str, help='Type of the database. E.g. "postgres", "snowflake".')
@click.option('--user', default=None, type=str, help='Connection username.')
@click.option('--password', default=None, type=str, help='Connection password.')
@click.option('--password-stdin', default=False, is_flag=True, help='Read connection password from standard input.')
@click.option('--host', default=None, type=str, help='Connection hostname.')
@click.option('--port', default=None, type=int, help='Connection port.')
@click.option('--database', default=None, type=str, help='Connection database name.')
@click.option('--schema', default=None, type=str, help='Connection schema name.')
@click.option('--warehouse', default=None, type=str, help='Connection warehouse name. (Used by Snowflake)')
@click.option('--account', default=None, type=str, help='Connection account name. (Used by Snowflake)')
@click.option('--project', default=None, type=str, help='Connection project name. (Used by BigQuery)')
@click.option('--key-file', default=None, type=str, help='Keyfile path to Service Account JSON. (Used by BigQuery)')
@click.option('--no-test', default=False, is_flag=True, help='Do NOT try test the connection.')
def create(
    name: str,
    type: Optional[str],
    user: Optional[str],
    host: Optional[str],
    port: Optional[int],
    password: Optional[str],
    password_stdin: bool,
    database: Optional[str],
    schema: Optional[str],
    warehouse: Optional[str],
    account: Optional[str],
    project: Optional[str],
    key_file: Optional[str],
    no_test: bool,
):
    """Add new connection.

    pano connection create postgres-prod --type postgres --user my_user \\
     --password-stdin --host localhost --port 5432 --database my_db
    """
    from panoramic.cli.connections import create_connection_command

    create_connection_command(
        name=name,
        type=type,
        user=user,
        host=host,
        port=port,
        password=password,
        password_stdin=password_stdin,
        database=database,
        schema=schema,
        warehouse=warehouse,
        account=account,
        project=project,
        key_file=key_file,
        no_test=no_test,
    )


@connection.command(cls=ConnectionAwareCommand)
@click.argument('name', type=str)
@click.option('--type', default=None, type=str, help='Type of the database. E.g. "postgres", "snowflake".')
@click.option('--user', default=None, type=str, help='Connection username.')
@click.option('--password', default=None, type=str, help='Connection password.')
@click.option('--password-stdin', default=False, is_flag=True, help='Read connection password from standard input.')
@click.option('--host', default=None, type=str, help='Connection hostname.')
@click.option('--port', default=None, type=int, help='Connection port.')
@click.option('--database', default=None, type=str, help='Connection database name.')
@click.option('--schema', default=None, type=str, help='Connection schema name.')
@click.option('--warehouse', default=None, type=str, help='Connection warehouse name. (Used by Snowflake)')
@click.option('--account', default=None, type=str, help='Connection account name. (Used by Snowflake)')
@click.option('--project', default=None, type=str, help='Connection project name. (Used by BigQuery)')
@click.option('--key-file', default=None, type=str, help='Keyfile path to Service Account JSON. (Used by BigQuery)')
@click.option('--no-test', default=False, is_flag=True, help='Do NOT try test the connection.')
def update(
    name: str,
    type: Optional[str],
    user: Optional[str],
    host: Optional[str],
    port: Optional[int],
    password: Optional[str],
    password_stdin: bool,
    database: Optional[str],
    schema: Optional[str],
    warehouse: Optional[str],
    account: Optional[str],
    project: Optional[str],
    key_file: Optional[str],
    no_test: bool,
):
    """Update existing connection.

    pano connection update postgres-prod --database my_new_prod_db
    """
    from panoramic.cli.connections import update_connection_command

    update_connection_command(
        name=name,
        type=type,
        user=user,
        host=host,
        port=port,
        password=password,
        password_stdin=password_stdin,
        database=database,
        schema=schema,
        warehouse=warehouse,
        account=account,
        project=project,
        key_file=key_file,
        no_test=no_test,
    )


@connection.command(cls=ConnectionAwareCommand)
@click.argument('name', type=str)
def remove(name: str):
    """Remove existing connection.

    pano connection remove postgres-prod
    """
    from panoramic.cli.connections import remove_connection_command

    remove_connection_command(name)


@connection.command(name='list', cls=ConnectionAwareCommand)
@click.option('--show-password', default=False, is_flag=True, help='Show passwords.')
def list_(show_password: bool):  # we cannot have method 'list' due to conflicts
    """List all available connections.

    pano connection list --show-password
    """
    from panoramic.cli.connections import list_connections_command

    list_connections_command(show_password)


@connection.command(cls=ConnectionAwareCommand)
@click.argument('name', default=None, type=str, required=False)
def test(name: str):
    """Test connections.

    pano connection test
    """
    from panoramic.cli.connections import test_connections_command

    test_connections_command(name)


@cli.group()
def analytics():
    """Analytics subcommand for managing anonymous usage metrics collection preferences."""
    pass


@analytics.command(name='on', cls=ConfigAwareCommand)
def analytics_on():
    """Opt in to anonymous usage analytics.

    pano analytics on
    """
    from panoramic.cli.analytics import opt_in_command

    opt_in_command()


@analytics.command(name='off', cls=ConfigAwareCommand)
def analytics_off():
    """Opt out off anonymous usage analytics.

    pano analytics off
    """
    from panoramic.cli.analytics import opt_out_command

    opt_out_command()


@analytics.command(name='id', cls=ConfigAwareCommand)
def analytics_id():
    """Display current anonymous usage tracking id.

    pano analytics id
    """
    from panoramic.cli.analytics import show_tracking_id_command

    show_tracking_id_command()


@cli.command(context_settings=dict(ignore_unknown_options=True), cls=DbtCommand)
@click.argument('dbt_args', nargs=-1, type=click.UNPROCESSED)
def dbt(dbt_args: Tuple[str]):
    """Run DBT command.

    \b
    Supported commands:
      pano dbt deps     Update package dependencies from pano.yaml
      pano dbt compile  Compile pre-model transforms into .dbt/target/compiled
      pano dbt run      Run compiled transforms against target defined in pano.yaml
    """
    # Handle no args passed
    if len(dbt_args) == 0:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        return

    args = list(dbt_args)

    if _PROFILES_DIR_ARG not in args:
        args.extend([_PROFILES_DIR_ARG, str(Paths.dbt_config_dir())])
    if _PROJECT_DIR_ARG not in args:
        args.extend([_PROJECT_DIR_ARG, str(Paths.dbt_project_dir())])

    return dbt_main(args=args)
