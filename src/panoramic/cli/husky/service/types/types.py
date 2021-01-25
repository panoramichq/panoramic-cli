from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import schematics
from schematics.types import (
    BaseType,
    DateTimeType,
    DictType,
    IntType,
    ListType,
    ModelType,
    StringType,
)
from sqlalchemy.sql import Selectable

from panoramic.cli.husky.core.model.enums import ValueQuantityType
from panoramic.cli.husky.core.schematics.model import EnumType, NonEmptyStringType
from panoramic.cli.husky.core.sql_alchemy_util import safe_identifier
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingDataRequest,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


class EffectivelyUsedModels(schematics.Model):
    """
    Structure holding information about models used to generate a query
    """

    noncached_models: List[str] = ListType(NonEmptyStringType(), min_size=1)
    """
    Noncached models
    """


class QueryDefinition(schematics.Model):
    query_string: str = StringType()
    """
    Final(Whole) query string used to retrieve data
    """

    query_id: str = StringType()
    """
    Query ID as returned by Snowflake, that can be used to retrieve history
    """

    taxons_selected: List[str] = ListType(StringType)
    """
    Which taxons were selected by the query
    """

    effectively_used_models: Dict[str, List[str]] = ModelType(EffectivelyUsedModels)
    """
    What models were used to generate the query
    """


class QueryInternalMetrics(schematics.Model):
    """
    Timings by each of the Husky components
    """

    taxon_manager_time_elapsed: int = IntType()
    model_retrieval_time_elapsed: int = IntType()
    graph_builder_time_elapsed: int = IntType()
    select_builder_time_elapsed: int = IntType()
    projection_builder_time_elapsed: int = IntType()
    filter_builder_time_elapsed: int = IntType()

    query_builder_time_elapsed: int = IntType()
    query_executor_time_elapsed: int = IntType()

    get_snowflake_query_info_time_elapsed: int = IntType()

    row_count: int = IntType()


class QueryInfo(schematics.Model):
    uuid: str = StringType(required=True)

    token_name: Optional[str] = StringType(required=False)
    """
    Slug of the token which was used to query the data. The field is intentionally not required in case we don't know
    the slug for whatever reason.
    """

    # TODO v1 use N/A when it is part of blending request....
    status: str = StringType(choices=['success', 'fail'], default='success')
    error: Optional[str] = StringType()

    data_request: InternalDataRequest = ModelType(InternalDataRequest)
    """
    Full API request
    """

    definition: QueryDefinition = ModelType(QueryDefinition, default=QueryDefinition())
    """
    Definition of the Query
    Similar to actual API request, but should have more stable structure, and be independent of API changes
    """

    internal_metrics: QueryInternalMetrics = ModelType(QueryInternalMetrics, default=QueryInternalMetrics())
    """
    Internal measurements.
    Mostly timings and counts
    """

    snowflake_metrics: Dict[str, Any] = DictType(BaseType)
    """
    Metrics retrieved from Snowflake Query History API
    """

    used_raw_taxons: List[str] = ListType(StringType(), default=[])
    """
    List of all raw taxons used by this subrequest
    """

    @staticmethod
    def create(data_request: InternalDataRequest):
        return QueryInfo(
            dict(
                data_request=data_request,
            )
        )


class BlendingQueryInfo(schematics.Model):
    """
    Query info object for blending requests.
    """

    uuid: str = StringType(required=True)

    start_time: datetime = DateTimeType()
    """
    Timestamp of the moment when Husky started processing the blending request
    """

    status: str = StringType(choices=['success', 'fail'], default='success')
    """
    Status of the request
    """

    error: Optional[str] = StringType()
    """
    Traceback to the error
    """

    data_request: BlendingDataRequest = ModelType(BlendingDataRequest)
    """
    Full API request
    """

    origin_information: Optional[Dict[str, Any]] = DictType(BaseType)
    """
    Contains additional (and optional) information about request origin
    """

    subrequests_info: List[QueryInfo] = ListType(ModelType(QueryInfo), default=[])
    """
    List of QueryInfo objects for subrequests
    """

    comparison_subrequests_info: List[QueryInfo] = ListType(ModelType(QueryInfo), default=[])
    """
    List of QueryInfo objects for comparison subrequests
    """

    internal_metrics: QueryInternalMetrics = ModelType(QueryInternalMetrics, default=QueryInternalMetrics())
    """
    Internal measurements.
    Mostly timings and counts
    """

    definition: QueryDefinition = ModelType(QueryDefinition, default=QueryDefinition())
    """
    Definition of the Query
    Similar to actual API request, but should have more stable structure, and be independend of API changes
    """

    original_request_str: str = StringType(required=False)
    """
    Original request as it came to API serialized into string.
    """

    query_runtime: HuskyQueryRuntime = EnumType(HuskyQueryRuntime)

    @staticmethod
    def create(
        data_request: BlendingDataRequest,
        husky_context: HuskyQueryContext,
        origin_information: Optional[Dict[str, Any]] = None,
    ):
        return BlendingQueryInfo(
            trusted_data=dict(
                data_request=data_request,
                start_time=datetime.utcnow(),
                origin_information=origin_information,
                query_runtime=husky_context.query_runtime,
            )
        )


class DataframeColumn:
    name: TaxonExpressionStr
    taxon: Taxon
    quantity_type: ValueQuantityType

    def __init__(
        self, name: TaxonExpressionStr, taxon: Taxon, quantity_type: ValueQuantityType = ValueQuantityType.scalar
    ):
        self.name = safe_identifier(name)
        self.taxon = taxon
        self.quantity_type = quantity_type


class Dataframe:
    """
    Holder class for sql alchemy query and taxons returned from that query.
    """

    query: Selectable
    """
    Sql alchemy select query
    """

    slug_to_column: Dict[TaxonExpressionStr, DataframeColumn]

    used_model_names: Set[str]
    """
    All the model names this dataframe is using.
    """

    used_physical_data_sources: Set[str]
    """
    Physical data sources used in this dataframe
    """

    def __init__(
        self,
        query: Selectable,
        slug_to_column: Dict[TaxonExpressionStr, DataframeColumn],
        used_model_names: Set[str],
        used_physical_data_sources: Set[str],
    ):
        self.query = query
        self.slug_to_column = slug_to_column
        self.used_model_names = used_model_names
        self.used_physical_data_sources = used_physical_data_sources

    @staticmethod
    def dataframe_columns_to_map(df_columns: List[DataframeColumn]) -> Dict[TaxonExpressionStr, DataframeColumn]:
        return {df.taxon.slug_expr: df for df in df_columns}

    def __repr__(self):
        return (
            "DataFrame("
            "query={repr(compile_query(self.query))}, "
            "slug_to_column={repr(self.slug_to_column)}, "
            "used_model_names={repr(self.used_model_names)}, "
            "used_physical_data_sources={repr(self.used_physical_data_sources)})"
        )


TaxonsByDataSource = Dict[str, List[Taxon]]
TaxonSlugsByDataSource = Dict[Optional[str], Set[TaxonExpressionStr]]
