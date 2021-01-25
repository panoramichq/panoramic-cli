from typing import Collection, Iterable, Optional

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.errors import BaseDieselException
from panoramic.cli.husky.core.taxonomy.override_mapping.constants import (
    MAX_OVERRIDE_MAPPINGS,
)


class OverrideMappingGenericError(BaseDieselException):
    """
    Exception covering generic problems with override mappings
    """

    def __init__(self, msg: str, slugs: Optional[Iterable[str]] = None):
        super().__init__(
            ExceptionErrorCode.OVERRIDE_GENERIC,
            msg,
            exception_group=ExceptionGroup.FDQ_TAXONOMY,
        )
        self._severity = ExceptionSeverity.info


class TooManyOverrideMappings(BaseDieselException):
    """
    Exception covering problem when too many override mappings were requested
    """

    def __init__(self, slugs: Iterable[str]):

        super().__init__(
            ExceptionErrorCode.OVERRIDE_TOO_MANY,
            (
                f'Too many override mappings are present in this request. '
                f'At most {MAX_OVERRIDE_MAPPINGS} mappings can be requested'
            ),
            component_type=ComponentType.HUSKY_API,
            exception_group=ExceptionGroup.TAXONS,
        )


class OverrideMappingsNotFound(BaseDieselException):
    """
    Exception covering case when override mapping was not found
    """

    def __init__(self, slugs: Collection[str]):
        super().__init__(
            ExceptionErrorCode.OVERRIDE_NOT_FOUND,
            'Override mapping(s) not found',
            exception_group=ExceptionGroup.FDQ_TAXONOMY,
        )
        self._severity = ExceptionSeverity.info
