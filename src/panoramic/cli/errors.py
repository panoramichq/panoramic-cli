import functools
import sys
from pathlib import Path
from typing import Callable, Optional

from requests.exceptions import RequestException

from panoramic.cli.logging import echo_error

DIESEL_REQUEST_ID_HEADER = 'x-diesel-request-id'


class CliBaseException(Exception):
    request_id: Optional[str] = None

    def add_request_id(self, request_id: str):
        self.request_id = request_id
        return self

    def extract_request_id(self, exc: RequestException):
        headers = getattr(exc.response, 'headers', {})
        return self.add_request_id(headers.get(DIESEL_REQUEST_ID_HEADER))

    def __str__(self) -> str:
        if self.request_id is not None:
            return f'{super().__str__()} (RequestId: {self.request_id})'
        return super().__str__()

    def __repr__(self) -> str:
        if self.request_id is not None:
            return f'{super().__repr__()} (RequestId: {self.request_id})'
        return super().__repr__()


class TimeoutException(CliBaseException):

    """Thrown when a remote operation times out."""


class MissingContextFileException(CliBaseException):

    """Generic missing context file error."""


class MissingConfigFileException(CliBaseException):

    """Generic missing config file error."""


class MissingValueException(CliBaseException):

    """Missing value in Yaml file"""

    def __init__(self, value_name: str):
        super().__init__(f'Missing value: {value_name}')


class RefreshException(CliBaseException):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Metadata could not be refreshed for table {table_name} in data connection {source_name}')


class SourceNotFoundException(CliBaseException):

    """Thrown when a source cannot be found."""

    def __init__(self, source_name: str):
        super().__init__(f'Data connection {source_name} not found. Has it been connected?')


class ScanException(CliBaseException):

    """Error scanning metadata."""

    def __init__(self, source_name: str, table_filter: Optional[str]):
        table_msg = f' {table_filter} ' if table_filter is not None else ' '
        super().__init__(f'Metadata could not be scanned for table(s){table_msg}in data counnection: {source_name}')


class VirtualDataSourceException(CliBaseException):

    """Error fetching virtual data sources."""

    def __init__(self, company_slug: str):
        super().__init__(f'Error fetching datasets for company {company_slug}')


class ModelException(CliBaseException):

    """Error fetching models."""

    def __init__(self, company_slug: str, dataset_name: str):
        super().__init__(f'Error fetching models for company {company_slug} and dataset {dataset_name}')


class InvalidYamlFile(CliBaseException):

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
