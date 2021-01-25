from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
)
from panoramic.cli.husky.core.errors import BaseDieselException


class GenericModelException(BaseDieselException):
    """
    Exception covering generic problems with HuskyModel
    """

    def __init__(
        self,
        msg: str,
        model_slug: str,
        error_code: ExceptionErrorCode,
        component: ComponentType = ComponentType.UNKNOWN,
    ):
        super().__init__(
            error_code,
            msg,
            component_type=component,
            exception_group=ExceptionGroup.MODEL,
        )
