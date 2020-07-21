from click.exceptions import ClickException
from yaml.scanner import ScannerError


class TimeoutException(Exception):

    """Thrown when a remote operation times out."""


class SourceNotFoundException(Exception):

    """Thrown when a source cannot be found."""


class ScanException(Exception):

    """Generic scanning error."""


class RefreshException(Exception):

    """Generic refresh error."""


class UnexpectedTablesException(Exception):

    """Generic unexpected table error."""


class MissingSchemaException(Exception):

    """Generic missing schema error."""


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


class PartialError(ClickException):

    """Command execution can continue."""
