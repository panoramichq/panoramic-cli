import inspect
import logging
from typing import Any, Dict, Optional

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.common.util import exception_to_string_with_traceback

logger = logging.getLogger(__name__)


class ExceptionTags:
    REQUEST_DATA = '_request_data'


class ExceptionHandler:
    @classmethod
    def track_exception(
        cls,
        exc: Exception,
        exc_group: ExceptionGroup = ExceptionGroup.COMMON,
        message: Optional[str] = None,
        ddog_tags: Optional[Dict[str, Any]] = None,
        severity: ExceptionSeverity = ExceptionSeverity.error,
        component: ComponentType = ComponentType.UNKNOWN,
    ):
        """
        Attempt to have one fn logging to stderr, datadog
        Let's see how this works for us and we can change later or add it to python lib.
        """
        caller_frame = inspect.stack()[1]
        called_by = f'File {caller_frame.filename}, line {caller_frame.lineno}, in {caller_frame.function}'

        ddog_tags = ddog_tags or dict()
        ddog_tags['exception_type'] = type(exc).__name__
        ddog_tags['exception_group'] = exc_group.value
        ddog_tags['component'] = component.value
        ddog_tags['severity'] = severity.value

        all_tags = dict()
        all_tags.update(ddog_tags)

        request_data_str = '<not-set>'
        if ExceptionTags.REQUEST_DATA in all_tags:
            # Log request data separately, not inside tags, coz it adds one more level of json escaping and is even
            # crazier to read
            request_data_str = str(all_tags[ExceptionTags.REQUEST_DATA])
            del all_tags[ExceptionTags.REQUEST_DATA]

        logger.error(
            f'Message: {message} Called by: {called_by}. '
            f'Exception: {exception_to_string_with_traceback(exc)} Tags: {all_tags} '
            f'{request_data_str}'
        )
