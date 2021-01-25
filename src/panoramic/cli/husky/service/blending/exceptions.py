from panoramic.cli.husky.common.exception_enums import (
    ExceptionErrorCode,
    ExceptionGroup,
)
from panoramic.cli.husky.service.utils.exceptions import HuskyException


class InvalidComparisonRequest(HuskyException):
    """
    Exception covering case when comparison taxons are NULL when working with blending requests
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__(
            ExceptionErrorCode.ERROR_PARSING_COMPARISON_RULES,
            'Missing comparison taxons when requesting comparisons',
            exception_group=ExceptionGroup.API,
        )
