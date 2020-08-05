import functools
import sys
from pathlib import Path
from typing import Callable, Optional

from panoramic.cli.logging import echo_error


class TimeoutException(Exception):

    """Thrown when a remote operation times out."""


class MissingContextFileException(Exception):

    """Generic missing context file error."""


class MissingConfigFileException(Exception):

    """Generic missing config file error."""


class MissingValueException(Exception):

    """Missing value in Yaml file"""

    def __init__(self, value_name: str):
        super().__init__(f'Missing value: {value_name}')


class IdentifierException(Exception):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Identifiers could not be generated for table {table_name} in data connection {source_name}')


class RefreshException(Exception):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Metadata could not be refreshed for table {table_name} in data connection {source_name}')


class SourceNotFoundException(Exception):

    """Thrown when a source cannot be found."""

    def __init__(self, source_name: str):
        super().__init__(f'Data connection {source_name} not found. Has it been connected?')


class ScanException(Exception):

    """Error scanning metadata."""

    def __init__(self, source_name: str, table_filter: Optional[str]):
        table_msg = f' {table_filter} ' if table_filter is not None else ' '
        super().__init__(f'Metadata could not be scanned for table(s){table_msg}in data counnection: {source_name}')


class VirtualDataSourceException(Exception):

    """Error fetching virtual data sources."""

    def __init__(self, company_slug: str):
        super().__init__(f'Error fetching datasets for company {company_slug}')


class ModelException(Exception):

    """Error fetching models."""

    def __init__(self, company_slug: str, dataset_name: str):
        super().__init__(f'Error fetching models for company {company_slug} and dataset {dataset_name}')


class InvalidYamlFile(Exception):

    """YAML syntax error."""

    def __init__(self, path: Path):
        super().__init__(f'Error parsing YAML from file {path.absolute}')


def handle_exception(f: Callable):
    """Print exception and exit with error code."""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            echo_error('Internal error occurred', exc_info=True)
            sys.exit(1)

    return wrapped
