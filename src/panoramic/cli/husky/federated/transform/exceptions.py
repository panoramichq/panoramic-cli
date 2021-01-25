from panoramic.cli.husky.common.exception_enums import ComponentType, ExceptionErrorCode
from panoramic.cli.husky.core.errors import BaseDieselException
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime


class TransformException(BaseDieselException):
    def __init__(self, error_code: ExceptionErrorCode, msg: str):
        super().__init__(error_code, msg)
        self.component_type = ComponentType.FEDERATED


class UnsupportedDialectError(BaseDieselException):
    def __init__(self, dialect: HuskyQueryRuntime):
        super().__init__(
            ExceptionErrorCode.FDQ_UNSUPPORTED_DIALECT,
            'You are trying to use unsupported dialect',
            component_type=ComponentType.FEDERATED,
        )
