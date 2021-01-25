from typing import Dict, List, Optional, Set

from sqlalchemy import column, distinct, func, nullslast
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import literal, literal_column

from panoramic.cli.husky.core.model.enums import ValueQuantityType
from panoramic.cli.husky.core.sql_alchemy_util import sort_columns
from panoramic.cli.husky.core.taxonomy.enums import ORDER_BY_FUNCTIONS, AggregationType
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate
from panoramic.cli.husky.service.constants import HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.types.api_data_request_types import TaxonDataOrder
from panoramic.cli.husky.service.types.types import Dataframe, DataframeColumn
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    SlugExprTaxonMap,
    TaxonExpressionStr,
)


class ProjectionBuilder:
    """
    This component aggregates the data from this subquery and prepares them for blending.
    """

    _AGGREGATION_FUNCTIONS_MAP: Dict[AggregationType, func.Function] = {
        AggregationType.sum: func.SUM,
        AggregationType.min: func.MIN,
        AggregationType.max: func.MAX,
        AggregationType.count_all: func.count,
        AggregationType.count_distinct: lambda col: func.count(distinct(col)),
    }
    """Map of aggregation functions applied at this layer"""

    _GROUP_BY_AGGREGATION_TYPES: Set[AggregationType] = {
        AggregationType.group_by,
        AggregationType.first_by,
        AggregationType.last_by,
    }
    """Set of aggregation types which should have their column in group by clause"""

    @classmethod
    def query(
        cls,
        select_query: Select,
        taxon_model_info_map: Dict[str, TaxonModelInfo],
        projection_taxons: SlugExprTaxonMap,
        data_source: str,
        order_by: Optional[List[TaxonDataOrder]],
        limit: Optional[int],
        offset: Optional[int],
        used_physical_data_sources: Set[str],
        dimension_templates: Optional[List[SqlFormulaTemplate]] = None,
    ) -> Dataframe:
        """
        Generates the final projected dataframe

        :param select_query: Original query fetching all necessary fields
        :param taxon_model_info_map: Map of taxon slug expression to taxon model info
        :param projection_taxons: List of taxons meant to be projected by the final query
        :param data_source: Virtual data source for this subrequest
        :param order_by: List of clauses for order by
        :param limit: Limit for the query
        :param offset: Offset for the query
        :param dimension_templates: List of dimension templates

        :return: Final dataframe including all requested taxons
        """
        group_by = []
        selectors = []

        projected_df_columns: Dict[TaxonExpressionStr, DataframeColumn] = {}
        for taxon in projection_taxons.values():
            # apply aggregation, if you need to
            agg_type = taxon.tel_metadata_aggregation_type
            if agg_type and agg_type in cls._AGGREGATION_FUNCTIONS_MAP:
                col = cls._AGGREGATION_FUNCTIONS_MAP[agg_type](column(taxon.slug_safe_sql_identifier))
            else:
                col = column(taxon.slug_safe_sql_identifier)

            col = col.label(taxon.slug_safe_sql_identifier)

            # create appropriate dataframe column
            value_quality_type = ValueQuantityType.scalar
            if not taxon.calculation and taxon.slug_expr in taxon_model_info_map:
                value_quality_type = taxon_model_info_map[taxon.slug_expr].quantity_type
            df_column_name = TaxonExpressionStr(taxon.slug)
            projected_df_columns[df_column_name] = DataframeColumn(df_column_name, taxon, value_quality_type)

            # make sure we select this column in the query
            selectors.append(col)

            # check whether this taxon should be in group by clause
            if agg_type in cls._GROUP_BY_AGGREGATION_TYPES:
                group_by.append(col)

        # make sure we select all columns for dimension templates
        for dim_template in dimension_templates or []:
            col = column(dim_template.label)
            selectors.append(col)

            # we should group by all dimension templates
            group_by.append(col)

        # On purpose adding this value to emulate USING ON FALSE => PROD-8136
        selectors.append(literal(data_source).label(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME))
        # using literal_column here because some database engines do not like grouping by constant
        group_by.append(literal_column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME))

        # created this query
        new_query = Select(
            columns=sort_columns(selectors),
            order_by=[nullslast(ORDER_BY_FUNCTIONS[item.type](item.taxon)) for item in (order_by or [])],
            group_by=sort_columns(group_by),
        ).select_from(select_query)

        if limit is not None:
            new_query = new_query.limit(limit)
        if offset is not None:
            new_query = new_query.offset(offset)

        # collect names of all used models
        used_model_names = {
            model_info.model_name for model_info in taxon_model_info_map.values() if model_info.model_name is not None
        }

        return Dataframe(new_query, projected_df_columns, used_model_names, used_physical_data_sources)
