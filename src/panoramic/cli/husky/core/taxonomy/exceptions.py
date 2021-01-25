from typing import Iterable, Optional

from panoramic.cli.husky.common.exception_enums import (
    ComponentType,
    ExceptionErrorCode,
    ExceptionGroup,
)
from panoramic.cli.husky.core.errors import BaseDieselException


class TaxonomyException(BaseDieselException):
    def __init__(
        self,
        error_code: ExceptionErrorCode,
        msg: str,
        *,  # following this will be only kwargs
        exception_group: ExceptionGroup = ExceptionGroup.API,
        root_exception: Optional[Exception] = None,
        component_type: Optional[ComponentType] = None,
    ):
        super().__init__(
            error_code,
            msg,
            exception_group=exception_group,
            root_exception=root_exception,
            component_type=component_type or ComponentType.TAXONOMY_API,
        )


class TaxonsNotFound(TaxonomyException):
    def __init__(self, taxon_slugs: Iterable[str]):
        super().__init__(
            ExceptionErrorCode.INACCESSIBLE_TAXON,
            f'Taxon(s) not found: {",".join(taxon_slugs)}',
            exception_group=ExceptionGroup.TAXONS,
        )


class UnexpectedTaxonsFound(TaxonomyException):
    def __init__(self, taxon_slugs: Iterable[str]):
        super().__init__(
            ExceptionErrorCode.UNEXPECTED_TAXONS,
            f'Unexpected taxon(s) found: {",".join(taxon_slugs)}',
            exception_group=ExceptionGroup.TAXONS,
        )
