from typing import Any, Dict, List, Optional

import schematics
from schematics.types import (
    BaseType,
    BooleanType,
    DictType,
    IntType,
    ListType,
    ModelType,
    PolyModelType,
    StringType,
)

from panoramic.cli.husky.common.my_memoize import memoized_property
from panoramic.cli.husky.core.schematics.model import (
    ApiModelSchematics,
    EnumType,
    NonEmptyStringType,
)
from panoramic.cli.husky.core.taxonomy.enums import TaxonOrderType
from panoramic.cli.husky.service.types.api_scope_types import (
    ApiScope,
    ComparisonScopeType,
    Scope,
)
from panoramic.cli.husky.service.types.util import get_all_filter_clauses
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


class TaxonDataOrder(schematics.Model):
    taxon: str = StringType(required=True, min_length=1)
    type: TaxonOrderType = EnumType(TaxonOrderType, required=True)


class DataRequestProperties(schematics.Model):
    # Model name, or data_sources, must be specified.
    data_sources: List[str] = ListType(NonEmptyStringType, default=list())
    model_name: Optional[str] = NonEmptyStringType()

    @memoized_property
    def data_source(self) -> str:
        """
        Shortcut for getting the data source
        """
        assert len(self.data_sources) == 1, 'Data sources is not of size 1'
        return list(self.data_sources)[0]


class DataRequestOrigin(schematics.Model):
    system: str = StringType(default='unknown')

    extra: Dict[str, Any] = DictType(BaseType)  # Any object caller wants to store.


class InternalDataRequest(schematics.Model):
    """Internal representation of data request"""

    taxons: List[TaxonExpressionStr] = ListType(NonEmptyStringType, required=False, default=[])

    preaggregation_filters = PolyModelType(get_all_filter_clauses())

    order_by: List[TaxonDataOrder] = ListType(ModelType(TaxonDataOrder), default=list())

    limit: Optional[int] = IntType()

    offset: Optional[int] = IntType()

    properties: DataRequestProperties = ModelType(DataRequestProperties, required=True)

    scope: Scope = ModelType(Scope, required=True)

    origin: Optional[DataRequestOrigin] = ModelType(DataRequestOrigin)

    physical_data_sources: Optional[List[str]] = ListType(NonEmptyStringType, required=False)
    """
    Data sources which should be considered when building the query.
    """


class ApiDataRequest(schematics.Model, ApiModelSchematics):
    """API representation of data request"""

    taxons: List[TaxonExpressionStr] = ListType(NonEmptyStringType, required=False, default=[])

    preaggregation_filters = PolyModelType(get_all_filter_clauses())

    order_by: List[TaxonDataOrder] = ListType(ModelType(TaxonDataOrder), default=list())

    limit: Optional[int] = IntType()

    offset: Optional[int] = IntType()

    properties: DataRequestProperties = ModelType(DataRequestProperties, required=True)

    scope: ApiScope = ModelType(ApiScope, required=True)

    origin: Optional[DataRequestOrigin] = ModelType(DataRequestOrigin)

    def to_internal_model(self) -> 'InternalDataRequest':
        return InternalDataRequest(self.to_primitive())


class SearchRequest(schematics.Model):
    taxons: List[TaxonExpressionStr] = ListType(StringType, required=True)

    properties: DataRequestProperties = ModelType(DataRequestProperties, required=True)

    scope: Scope = ModelType(Scope, required=True)


class BlendingSearchRequest(schematics.Model):
    data_subrequests: List[ApiDataRequest] = ListType(ModelType(ApiDataRequest), min_size=1)
    taxons: List[TaxonExpressionStr] = ListType(NonEmptyStringType, required=False)


class ComparisonConfig(schematics.Model):
    taxons: Optional[List[TaxonExpressionStr]] = ListType(NonEmptyStringType)
    scope: ComparisonScopeType = EnumType(ComparisonScopeType, default=ComparisonScopeType.company)


GroupingSets = ListType(ListType(StringType(required=True, min_length=1), required=True), required=False)


class BlendingDataRequest(schematics.Model):
    data_subrequests: List[ApiDataRequest] = ListType(ModelType(ApiDataRequest), min_size=1)

    taxons: List[TaxonExpressionStr] = ListType(NonEmptyStringType, required=False)
    """
    List of taxons to fetch. Pushed to individual subrequests based on the data source.
    """

    order_by: List[TaxonDataOrder] = ListType(ModelType(TaxonDataOrder), default=list())
    """
    Specify order of the final blended data frame.
    Taxons used here are not automatically propagated into subrequest.
    Example: if you use order by for taxonA, but don't specify the taxonA in *any* of the subrequests, it will fail,
    coz the taxonA won't be available and possible to sort on. We cannot propagate the taxonA to all subrequests
    automatically, because there are cases where you don't want to select taxonA from all subrequests, but just from
    some.
    """

    limit: Optional[int] = IntType()

    origin: DataRequestOrigin = ModelType(DataRequestOrigin)

    comparison: Optional[ComparisonConfig] = ModelType(ComparisonConfig, required=False)
    grouping_sets: Optional[List[List[TaxonExpressionStr]]] = GroupingSets
    """
    Groups data exactly as defined on https://docs.snowflake.net/manuals/sql-reference/constructs/group-by-grouping-sets.html#group-by-grouping-sets
    NULL values produced by the aggregation are encoded as "PANORAMIC_GROUPINGSETS_NULL" (string).
    """

    fill_date_gaps: Optional[bool] = BooleanType(required=False, default=False)

    filters = PolyModelType(get_all_filter_clauses())  # Omitting type here to avoid circular dependencies
    """
    Typically metric filters, applied after all computations and aggregations.
    """

    physical_data_sources: Optional[List[str]] = ListType(NonEmptyStringType, required=False)
    """
    Data sources which should be considered when building the query.
    """
