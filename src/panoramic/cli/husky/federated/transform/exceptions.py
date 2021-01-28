from panoramic.cli.husky.common.exception_enums import ComponentType, ExceptionErrorCode
from panoramic.cli.husky.core.errors import BaseDieselException


class TransformException(BaseDieselException):
    def __init__(self, error_code: ExceptionErrorCode, msg: str):
        super().__init__(error_code, msg)
        self.component_type = ComponentType.FEDERATED


class UnsupportedDialectError(BaseDieselException):
    def __init__(self, dialect: str):
        super().__init__(
            ExceptionErrorCode.FDQ_UNSUPPORTED_DIALECT,
            f'You are trying to use unsupported dialect - "{dialect}"',
            component_type=ComponentType.FEDERATED,
        )
