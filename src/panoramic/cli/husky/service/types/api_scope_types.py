from enum import Enum
from typing import Optional

import schematics
from schematics.types import PolyModelType, StringType

from panoramic.cli.husky.core.model.enums import ModelVisibility
from panoramic.cli.husky.core.schematics.model import EnumType
from panoramic.cli.husky.service.types.util import get_all_filter_clauses


class DataSourceAccount(schematics.Model):
    data_source: str = StringType(required=True, min_length=1)
    account_id: str = StringType(required=True, min_length=1)


class ComparisonScopeType(Enum):
    company = 'company'


class Scope(schematics.Model):

    company_id: str = StringType(required=True)
    """
    Historically, company_id=None meant global. Now, we should use support company id, same as with taxons.
    """
    project_id: Optional[str] = StringType()

    model_visibility: ModelVisibility = EnumType(ModelVisibility, default=ModelVisibility.available)
    """
    Every production consumer should not set this field. Set to experimental if you want to test new changes / models.
    """

    preaggregation_filters = PolyModelType(get_all_filter_clauses())
    """Optional pre-aggregation filters which determine scope of this Husky request"""

    @property
    def all_filters(self):
        """
        Get current preaggregation scope filters
        """
        return self.preaggregation_filters

    def __hash__(self) -> int:
        return hash(
            '_'.join(
                str(part)
                for part in [
                    self.project_id,
                    self.company_id,
                    self.model_visibility,
                ]
            )
        )


class ApiScope(schematics.Model):
    company_id: str = StringType()
    project_id: Optional[str] = StringType()

    model_visibility: ModelVisibility = EnumType(ModelVisibility, default=ModelVisibility.available)
    """
    Every production consumer should not set this field. Set to experimental if you want to test new changes / models.
    """

    preaggregation_filters = PolyModelType(get_all_filter_clauses())
    """Pre-aggregation filters which determine scope of this Husky request"""
