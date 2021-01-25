from panoramic.cli.husky.common.exception_enums import ExceptionErrorCode
from panoramic.cli.husky.core.errors import BaseDieselException


class ModelTelExpressionException(BaseDieselException):
    """
    Generic exception fired during parsing Model Attribute Transformation TEL parsing
    """

    def __init__(self, message: str):
        super().__init__(error_code=ExceptionErrorCode.INVALID_MODEL_TEL_EXPRESSION, msg=message)

    @classmethod
    def create_with_details(cls, msg: str, position: int, line: int, expression: str):
        full_msg = f'{msg}. Occurred at position {position}, line {line} in expression "{expression}"'
        return cls(message=full_msg)


class ModelTelCyclicReferenceException(BaseDieselException):
    """
    Model transformation TEL contains cyclic reference
    """

    def __init__(self, attribute_name: str):
        super().__init__(
            error_code=ExceptionErrorCode.CYCLIC_REFERENCE_MODEL_ATTR_TEL,
            msg=f'TEL transformation in attribute "{attribute_name}" causes cyclic reference within the model.',
        )
