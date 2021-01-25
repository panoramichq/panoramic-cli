from panoramic.cli.husky.common.exception_enums import (
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.errors import BaseDieselException


class WrongVirtualDataSource(BaseDieselException):
    """
    Exception covering case when an attribute has incorrect virtual data source (or is missing it)
    """

    def __init__(self, virtual_data_source: str, attr_val: str, msg: str):
        """

        :param attr_val: Attribute value
        :param virtual_data_source: Wanted virtual data source
        :param msg: FE message
        """
        super().__init__(
            ExceptionErrorCode.WRONG_VIRTUAL_DATA_SOURCE_ATTR,
            msg,
            exception_group=ExceptionGroup.FDQ_MODEL_ERROR,
        )
        self._severity = ExceptionSeverity.info
