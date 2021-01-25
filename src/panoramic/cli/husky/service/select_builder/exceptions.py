from typing import Optional, Set

from panoramic.cli.husky.common.exception_enums import (
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.service.utils.exceptions import HuskyException
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonSlugExpression


class UnsupportedAggregationType(HuskyException):
    """
    Exception covering case when taxon has an unsupported aggregation type
    """

    def __init__(self, taxon: Taxon):
        """
        Constructor

        :param taxon: Affected taxon
        """
        super().__init__(
            ExceptionErrorCode.TAXON_ISSUE,
            'Internal error has occurred',
            exception_group=ExceptionGroup.UNSUPPORTED,
        )
        self._severity = ExceptionSeverity.info


class ImpossibleTaxonCombination(HuskyException):
    """
    Exception covering case when asking for impossible combination of taxons
    (no combination of models can accommodate it)
    """

    def __init__(
        self,
        taxon_slugs_expressions: Set[TaxonSlugExpression],
        data_source: Optional[str] = None,
        allowed_physical_data_sources: Optional[Set[str]] = None,
    ):
        """
        Constructor

        :param taxon_slugs_expressions: Set of wanted taxon slugs
        """
        if allowed_physical_data_sources:
            msg = (
                f'Impossible combination of taxons on physical data sources {", ".join(allowed_physical_data_sources)}'
            )
        elif data_source:
            msg = f'Impossible combination of taxons on data source {data_source}'
        else:
            msg = 'Impossible combination of taxons'
        super().__init__(
            ExceptionErrorCode.IMPOSSIBLE_TAXON_COMBINATION,
            msg,
            exception_group=ExceptionGroup.TAXONS,
        )
