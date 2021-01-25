from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.errors import BaseDieselException


class DetectJoinsException(BaseDieselException):
    def __init__(self, error_code: ExceptionErrorCode, msg: str):
        super().__init__(error_code, msg)
        self.component_type = ComponentType.FEDERATED


class DetectJoinsJobNotFound(DetectJoinsException):
    def __init__(self, job_id: str):
        super().__init__(ExceptionErrorCode.FDQ_JOIN_DETECTION_JOB_NOT_FOUND, 'Suggest joins job not found')
        self.severity = ExceptionSeverity.warning
