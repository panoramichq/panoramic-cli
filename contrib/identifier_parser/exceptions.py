from typing import List

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.errors import BaseDieselException
from panoramic.cli.husky.federated.identifier_parser.enums import ColumnOverflowStrategy


class IdentifierParserException(BaseDieselException):
    def __init__(self, error_code: ExceptionErrorCode, msg: str):
        super().__init__(error_code, msg)
        self.component_type = ComponentType.FEDERATED


class SampleJobError(IdentifierParserException):
    def __init__(self, job_id: str):
        self._msg = 'Parser job errord out'
        self.severity = ExceptionSeverity.error


class SampleJobTimeout(IdentifierParserException):
    def __init__(self, job_id: str):
        self._msg = 'Parser job timed out'
        self.severity = ExceptionSeverity.error


class ParserJobNotFound(IdentifierParserException):
    def __init__(self, job_id: str):
        self._msg = 'Parser job not found'
        self.severity = ExceptionSeverity.warning


class ServiceUnavailable(IdentifierParserException):
    def __init__(self, physical_data_source: str, table_name: str, columns: List[str]):
        self._msg = (
            f'Worker and/or scheduler are not available (data source {physical_data_source} and table {table_name})'
        )

        self._exception_group = ExceptionGroup.FDQ_IDENTIFIERS


class TooManyColumns(IdentifierParserException):
    def __init__(self, physical_data_source: str, table_name: str, columns: List[str]):
        self._msg = (
            f'Too many columns to run id parser job for data source {physical_data_source} and table {table_name}'
        )

        self._exception_group = ExceptionGroup.FDQ_IDENTIFIERS


class ColumnMetadataNotAvailable(IdentifierParserException):
    def __init__(self, physical_data_source: str, table_name: str):
        self._msg = f'No column metadata available for data source {physical_data_source} and table {table_name}'

        self._exception_group = ExceptionGroup.FDQ_IDENTIFIERS


class UnknownStrategy(IdentifierParserException):
    def __init__(
        self, strategy: ColumnOverflowStrategy, physical_data_source: str, table_name: str, columns: List[str]
    ):
        self._msg = f'Unknown strategy `{strategy}` received while trying to handle extraneous columns'

        self._exception_group = ExceptionGroup.FDQ_IDENTIFIERS


class ConfigurationError(IdentifierParserException):
    def __init__(self):
        self._msg = 'Configuration error'
        self.severity = ExceptionSeverity.error


class MissingConfiguration(ConfigurationError):
    def __init__(self):
        self._msg = 'Failed to retrieve configuration from Consul'


class InvalidConfiguration(ConfigurationError):
    def __init__(self, message: str):
        self._msg = f'Could not parse configuration from Consul due to: {message}'
