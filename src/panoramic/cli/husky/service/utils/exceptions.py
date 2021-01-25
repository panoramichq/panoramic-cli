from typing import Any, Dict, List, Optional

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.errors import BaseDieselException
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException


class HuskyException(BaseDieselException):
    def __init__(
        self,
        error_code: ExceptionErrorCode,
        msg: str,
        *,  # following this will be only kwargs
        exception_group: ExceptionGroup = ExceptionGroup.API,
        root_exception: Optional[Exception] = None,
    ):
        super().__init__(
            error_code,
            msg,
            exception_group=exception_group,
            root_exception=root_exception,
        )
        self._scope = None  # scope of the request
        self._request_id: Optional[str] = None
        self._component_type = ComponentType.HUSKY_API

    def track_exception(self):
        if self._scope:
            self._ddog_tags.update({'project_id': self._scope.project_id, 'company_id': self._scope.company_id})

        super().track_exception()

    def set_scope(self, scope):
        """
        Sets scope of the request which threw the exception
        """
        self._scope = scope


class MissingJoinTaxons(HuskyException):
    """
    Exception representing missing taxons on joins
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__(ExceptionErrorCode.MISSING_TAXON_JOIN, 'Invalid model', exception_group=ExceptionGroup.MODEL)


class HuskyInvalidTelException(HuskyException):
    """
    Exception covering all exception raise during parsing math expression
    """

    def __init__(self, inner_exception: TelExpressionException, taxon_slug: str):
        """
        Constructor

        :param inner_exception: Original exception from MathExpressionParser
        :param taxon_slug: Affected taxon
        """
        message = f'Invalid expression on taxon {taxon_slug}'
        super().__init__(
            ExceptionErrorCode.INVALID_TEL_EXPRESSION,
            message,
            exception_group=ExceptionGroup.TAXONS,
            root_exception=inner_exception,
        )


class ModelNotFoundException(HuskyException):
    """
    Exception covering case when there is no model to work with
    """

    def __init__(self, extra_data: Optional[Dict[str, Any]] = None):
        """
        Constructor
        """
        super().__init__(
            ExceptionErrorCode.NO_MODEL_FOUND,
            'Model not found',
            exception_group=ExceptionGroup.MODEL,
        )
        self._severity = ExceptionSeverity.info


class InvalidRequest(HuskyException):
    """
    Exception covering case when required identifier is missing from request
    """

    def __init__(self, identifier_name: str, msg: str, extra_response: Optional[Dict[str, Any]] = None):
        """
        Constructor

        :param identifier_name: Name of the identifier which cause the exception
        :param msg: User-friendly message for frontend
        :param extra_response:   Optional extra response data
        """
        super().__init__(
            ExceptionErrorCode.REQUEST_VALIDATION,
            msg,
            exception_group=ExceptionGroup.WRONG_FORMAT,
        )

        self._severity = ExceptionSeverity.info


class UnknownPhysicalDataSource(HuskyException):
    """
    Exception thrown when rendering a native query but Husky does not recognize the physical data source
    """

    def __init__(self, pds: str):
        """
        Constructor

        :param pds: Physical data source
        """
        msg = f'Unsupported physical data source for SQL output: {pds}'

        super().__init__(
            ExceptionErrorCode.UNKNOWN_PHYSICAL_DATA_SOURCE,
            msg,
            exception_group=ExceptionGroup.FDQ_TRANSFORM,
        )


class UnsupportedSQLOutputException(HuskyException):
    """
    Exception thrown when rendering a native query but Husky does not support the dialect for SQL output.
    """

    def __init__(self, dialect: str):
        """
        Constructor

        :param dialect: Set of physical data sources used in the query
        """
        msg = f'Unsupported dialect for SQL output: {dialect}'

        super().__init__(
            ExceptionErrorCode.FDQ_UNSUPPORTED_DIALECT,
            msg,
            exception_group=ExceptionGroup.FDQ_TRANSFORM,
        )


class TooManyPhysicalDataSourcesException(HuskyException):
    """
    Exception thrown when rendering a native query but a physical dialect cannot be deduced, due to having
    more than one physical data source in the query.
    """

    def __init__(self, physical_data_sources: List[str]):
        """
        Constructor

        :param physical_data_sources: Set of physical data sources used in the query
        """
        physical_data_sources_sorted = sorted(physical_data_sources)
        msg = f'Too many physical data sources: {",".join(physical_data_sources_sorted)}'

        super().__init__(
            ExceptionErrorCode.TOO_MANY_PHYSICAL_DATA_SOURCES,
            msg,
            exception_group=ExceptionGroup.TAXONS,
        )
