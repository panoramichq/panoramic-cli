from typing import Optional

from click.exceptions import ClickException
from yaml.scanner import ScannerError


class TimeoutException(Exception):

    """Thrown when a remote operation times out."""


class MissingContextFileException(Exception):

    """Generic missing context file error."""


class MissingConfigFileException(Exception):

    """Generic missing config file error."""


class InvalidYamlFile(Exception):

    """Yaml syntax error."""

    def __init__(self, error: ScannerError):
        super().__init__(str(error))


class MissingValueException(Exception):

    """Missing value in Yaml file"""

    def __init__(self, value_name: str):
        super().__init__(f'Missing value: {value_name}')


class CriticalError(ClickException):

    """Aborts command execution."""


class RefreshException(ClickException):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Metadata could not be refreshed for table {table_name} in data connection {source_name}')


class SourceNotFoundException(ClickException):

    """Thrown when a source cannot be found."""

    def __init__(self, source_name: str):
        super().__init__(f'Data connection {source_name} not found. Has it been connected?')


class ScanException(ClickException):

    """Error scanning metadata."""

    def __init__(self, source_name: str, table_filter: Optional[str]):
        table_msg = f' {table_filter} ' if table_filter is not None else ' '
        super().__init__(f'Metadata could not be scanned for table(s){table_msg}in data counnection: {source_name}')
