from panoramic.cli.husky.common.exception_enums import ExceptionErrorCode
from panoramic.cli.husky.core.errors import BaseDieselException


class TelExpressionException(BaseDieselException):
    def __init__(self, message: str):
        super().__init__(error_code=ExceptionErrorCode.INVALID_TEL_EXPRESSION, msg=message)

    @classmethod
    def create_with_message(cls, msg, position, line, expression):
        full_msg = f'{msg}. Occurred at position {position}, line {line} in expression "{expression}"'
        return cls(message=full_msg)


class MissingRequiredTaxonException(TelExpressionException):
    def __init__(self, message: str):
        super().__init__(message)
