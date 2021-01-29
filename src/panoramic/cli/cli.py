import logging
import sys
import warnings
from collections import defaultdict
from typing import IO, Optional, cast

import click
from click.core import Command, Context
from dotenv import load_dotenv

from panoramic.cli.__version__ import __version__
from panoramic.cli.errors import (
    SourceNotFoundException,
    ValidationError,
    ValidationErrorSeverity,
    handle_exception,
    handle_interrupt,
)
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_error, echo_errors, echo_info, echo_warnings

_PROFILES_DIR_ARG = '--profiles-dir'
_PROJECT_DIR_ARG = '--project-dir'


class ConnectionAwareCommand(Command):
    def invoke(self, ctx: Context):
        try:
            return super().invoke(ctx)
        except Exception as e:
            echo_error(str(e))
            sys.exit(1)


class ContextAwareCommand(Command):
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
        except (ValidationError, SourceNotFoundException) as e:
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

        # preload data for taxonomy and calculate TEL metadata
        from panoramic.cli.husky.core.taxonomy.getters import Taxonomy

        Taxonomy.preload_taxons_from_state()
        Taxonomy.precalculate_tel_metadata()

        return super().invoke(ctx)


@click.group(context_settings={'help_option_names': ["-h", "--help"]}, help='')
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
@handle_exception
def cli(debug):
    """Run checks at the beginning of every command."""
    if debug:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

    # hide unclosed socket errors
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<socket.socket.*>")

    load_dotenv(dotenv_path=Paths.dotenv_file())


@cli.command(help='Scan models from source', cls=ContextAwareCommand)
@click.option('--filter', '-f', type=str, help='Filter down what models to scan using regular expression')
@handle_exception
@handle_interrupt
def scan(filter: Optional[str]):
    from panoramic.cli.command import scan as scan_command

    scan_command(filter)


@cli.command(help='Validate local files', cls=Command)
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
    pass


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
@click.option('--no-remote', is_flag=True, default=False, help='Do not remote connection to determine data types')
@handle_exception
def scaffold(target_dataset: str, yes: bool, no_remote: bool):
    from panoramic.cli.command import (
        scaffold_missing_fields as scaffold_missing_fields_command,
    )

    scaffold_missing_fields_command(target_dataset=target_dataset, yes=yes, no_remote=no_remote)


@field_cli.command(help='My cmd', cls=LocalStateAwareCommand)
@handle_exception
def cmd():
    from panoramic.cli.husky.core.virtual_state.mappers import VirtualStateMapper
    from panoramic.cli.local import get_state

    state = get_state()
    internal_state = VirtualStateMapper.to_husky(state)
    print(internal_state.virtual_data_sources)

    from panoramic.cli.config.companies import get_company_id
    from panoramic.cli.husky.core.taxonomy.getters import Taxonomy

    Taxonomy.preload_taxons_from_state()
    Taxonomy.precalculate_tel_metadata()

    taxons = Taxonomy.get_taxons(get_company_id())
    print(taxons)


@cli.group()
def connection():
    """Connection subcommand for managing a connection.

    \b
    Connection is stored in the project pano.yaml file.
    You can edit this file either manually or using provided commands.

    Expected YAML structure:

    \b
    url: postgres://my_user@<password>@localhost:5432/my_db

    \b
    name: The name of the connection that will be used as reference to specify which connection to use.
    url: SqlAlchemy compatible connection URL.
    """
    pass


@connection.command(cls=ConnectionAwareCommand)
@click.option('--url', type=str, help='Connection URL (SqlAlchemy format).')
@click.option('--dialect', type=click.Choice(['snowflake', 'bigquery']), help='Dialect. Use when no URL is provided.')
@click.option(
    '--no-test', default=False, is_flag=True, help='Do NOT try test the connection. Only if the URL was specified'
)
def setup(
    url: str,
    dialect: str,
    no_test: bool,
):
    """Add new connection. Either a URL or a dialect must be provided.

    pano connection setup --url 'snowflake://{username}:{password}@{full account name}/{database_name}/{schema}'
    pano connection setup --dialect snowflake
    """
    from panoramic.cli.connection import setup_connection_command

    setup_connection_command(
        url=url,
        dialect=dialect,
        no_test=no_test,
    )


@connection.command(name='show', cls=ConnectionAwareCommand)
def show():
    """SHow the connection details.

    pano connection show
    """
    from panoramic.cli.connection import show_connection_command

    show_connection_command()


@connection.command(cls=ConnectionAwareCommand)
def test():
    """Test the connection.

    pano connection test
    """
    from panoramic.cli.connection import test_connection_command

    test_connection_command()


@connection.command(cls=ConnectionAwareCommand)
@click.argument('query', type=str, default=None, required=False)
@click.option('--file', type=click.File('rb'), nargs=1, help='File with the query.')
@click.option(
    '--type',
    type=click.Choice(['view', 'table', 'raw']),
    default='raw',
    nargs=1,
    help='Type of the object SQL query, either view, or table to store it in. Default is raw, meaning the provided query must take care of the table/view creation on its own.',
)
@click.option('--name', type=str, nargs=1, help='Optional name of the view or table to create using the query/file.')
def execute(
    query: Optional[str],
    file: Optional[click.File],
    type: Optional[str],
    name: Optional[str],
):
    """Execute a query or a file using the specified connection.

    pano connection execute --type view 'select * from table'
    """
    from panoramic.cli.connection import execute_command

    execute_command(
        name=name,
        query=query,
        file=cast(IO, file),
        type=type,
    )


@cli.group(name='transform')
def transform_cli():
    """Commands on local transform files."""
    pass


@transform_cli.command(name='create', help='Scaffold a new transform file', cls=ContextAwareCommand)
@handle_exception
def transform_create():
    from panoramic.cli.transform.commands import create_command

    create_command()


@transform_cli.command(name='exec', help='Execute transforms', cls=LocalStateAwareCommand)
@click.option('--yes', '-y', is_flag=True, default=False, help='Automatically confirm all actions')
@click.option(
    '--compile', 'compile_only', is_flag=True, default=False, help='Only compile transforms to sql statements'
)
@handle_exception
def transform_exec(yes: bool, compile_only: bool):
    from panoramic.cli.transform.commands import exec_command

    exec_command(yes=yes, compile_only=compile_only)
