from typing import Dict, Optional, Union

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.common.exception_handler import ExceptionHandler


class ExceptionWithSeverity(Exception):
    severity: ExceptionSeverity

    def __init__(self, msg: str, severity: ExceptionSeverity = ExceptionSeverity.error):
        super().__init__(msg)
        self.severity = severity


class BaseDieselException(ExceptionWithSeverity):
    """
    Base class for all exceptions thrown in all modules in Diesel. It contains the original message along with
    user-friendly message and error code

    :param error_code: Error code
    :param msg: User-friendly error message
    :param exception_group: Exception group
    :param root_exception: Defines root exception (in case this exception is triggered by another exception)
    """

    def __init__(
        self,
        error_code: ExceptionErrorCode,
        msg: str,
        *,  # following this will be only kwargs
        exception_group: ExceptionGroup = ExceptionGroup.API,
        root_exception: Optional[Exception] = None,
        component_type: Optional[ComponentType] = None,
    ):
        super().__init__(msg)
        self._msg: str = msg
        self._error_code: ExceptionErrorCode = error_code
        self._exception_group: ExceptionGroup = exception_group
        self._root_exception: Exception = root_exception or self

        self._ddog_tags: Dict[str, Optional[Union[str, float]]] = {}  # tags for Datadog measurement

        self.component_type: ComponentType = component_type or ComponentType.UNKNOWN

    def track_exception(self):
        """
        Tracks the exceptions
        """
        ExceptionHandler.track_exception(
            self._root_exception,
            self._exception_group,
            self._msg,
            self._ddog_tags,
            self.severity,
            self.component_type,
        )
