import functools
import os
import signal
import sys
from abc import ABC
from pathlib import Path
from typing import Callable, List, Optional

from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from requests.exceptions import RequestException
from yaml.error import MarkedYAMLError

from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_error

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


class IdentifierException(CliBaseException):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Identifiers could not be generated for table {table_name} in data connection {source_name}')


class JoinException(CliBaseException):

    """Error detecting joins in a dataset."""

    def __init__(self, dataset_name: str):
        super().__init__(f'Joins could not be detected for {dataset_name}')


class RefreshException(CliBaseException):

    """Error refreshing metadata."""

    def __init__(self, source_name: str, table_name: str):
        super().__init__(f'Metadata could not be refreshed for table {table_name} in data connection {source_name}')


class CompanyNotFoundException(CliBaseException):

    """Error when company not found for user (either no access or doesn't exist)."""

    def __init__(self, company_slug: str):
        super().__init__(f'Company {company_slug} not found. Do you have access to it?')


class SourceNotFoundException(CliBaseException):

    """Thrown when a source cannot be found."""

    def __init__(self, source_name: str):
        super().__init__(f'Data connection {source_name} not found. Has it been connected?')


class DatasetNotFoundException(CliBaseException):

    """Thrown when a dataset cannot be found."""

    def __init__(self, dataset_name: str):
        super().__init__(f'Dataset {dataset_name} not found. Has it been created?')


class ScanException(CliBaseException):

    """Error scanning metadata."""

    def __init__(self, source_name: str, table_filter: Optional[str]):
        table_msg = f' {table_filter} ' if table_filter is not None else ' '
        super().__init__(f'Metadata could not be scanned for table(s){table_msg}in data counnection: {source_name}')


class DatasetReadException(CliBaseException):

    """Error fetching virtual data sources."""

    def __init__(self, company_slug: str):
        super().__init__(f'Error fetching datasets for company {company_slug}')


class InvalidModelException(CliBaseException):

    """Invalid model submitted to remote."""

    messages: List[str]

    def __init__(self, error: RequestException):
        try:
            self.messages = [
                error['msg'] for error in error.response.json()['error']['extra_data']['validation_errors']
            ]
        except Exception:
            self.messages = ['Invalid model submitted']


class InvalidDatasetException(CliBaseException):

    """Invalid model submitted to remote."""

    messages: List[str]

    def __init__(self, error: RequestException):
        try:
            self.messages = [
                error['msg'] for error in error.response.json()['error']['extra_data']['validation_errors']
            ]
        except Exception:
            self.messages = ['Invalid dataset submitted']


class DatasetWriteException(CliBaseException):

    """Error writing dataset to remote state."""

    def __init__(self, dataset_name: str):
        super().__init__(f'Error writing dataset {dataset_name}')


class ModelWriteException(CliBaseException):

    """Error writing dataset to remote state."""

    def __init__(self, dataset_name: str, model_name: str):
        super().__init__(f'Error writing model {model_name} in dataset {dataset_name}')


class ModelReadException(CliBaseException):

    """Error reading model(s) from remote state."""

    def __init__(self, company_slug: str, dataset_name: str):
        super().__init__(f'Error fetching models for company {company_slug} and dataset {dataset_name}')


class ValidationError(CliBaseException, ABC):

    """Abstract error raised during validation step."""


class FileMissingError(ValidationError):

    """File that should exist didn't."""

    def __init__(self, *, path: Path):
        if path == Paths.context_file():
            msg = f'Context file ({path.name}) not found in current working directory. Run pano init to create it.'
        elif path == Paths.config_file():
            msg = f'Config file ({path.absolute()}) not found. Run pano configure to create it.'
        else:
            # Should not happen => we only check above files exist explicitly
            msg = f'File Missing - {path}'

        super().__init__(msg)


class DuplicateModelNameError(ValidationError):

    """Two local models use the same model name."""

    def __init__(self, *, model_name: str, paths: List[Path]) -> None:
        try:
            paths = [path.relative_to(Path.cwd()) for path in paths]
        except ValueError:
            pass  # Use relative path when possible

        path_lines = ''.join(f'\n  in {path}' for path in paths)
        super().__init__(f'Multiple model files use model name {model_name}{path_lines}')


class InvalidYamlFile(ValidationError):

    """YAML syntax error."""

    def __init__(self, *, path: Path, error: MarkedYAMLError):
        try:
            path = path.relative_to(Path.cwd())
        except ValueError:
            pass  # Use relative path when possible

        super().__init__(f'Invalid YAML file - {error.problem}\n  on line {error.problem_mark.line}\n  in {path}')


class JsonSchemaError(ValidationError):
    def __init__(self, *, path: Path, error: JsonSchemaValidationError):
        try:
            path = path.relative_to(Path.cwd())
        except ValueError:
            pass  # Use relative path when possible

        super().__init__(f'{error.message}\n  in {path}')


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


def handle_interrupt(f: Callable):
    """Exit app on keyboard interrupt."""

    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except KeyboardInterrupt:
            os._exit(128 + signal.SIGINT)

    return wrapped
