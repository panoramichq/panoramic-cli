from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import String, cast, column, nullslast
from sqlalchemy.sql import Select

from panoramic.cli.husky.core.model.enums import ValueQuantityType
from panoramic.cli.husky.core.sql_alchemy_util import safe_identifier, sort_columns
from panoramic.cli.husky.core.taxonomy.enums import ORDER_BY_FUNCTIONS
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.service.blending.types import ColumnAndDataframeColumn
from panoramic.cli.husky.service.types.api_data_request_types import TaxonDataOrder
from panoramic.cli.husky.service.types.types import Dataframe, DataframeColumn
from panoramic.cli.husky.service.utils.exceptions import (
    HuskyInvalidTelException,
    InvalidRequest,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


class ProjectionBuilder:
    @classmethod
    def project_dataframe(
        cls,
        calc_df: Dataframe,
        return_taxons: Dict[TaxonExpressionStr, Taxon],
        physical_data_sources: Set[str],
        order_by: Optional[List[TaxonDataOrder]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dataframe:
        """
        Applies in this order:
        - filtering
        - ordering
        - limiting and offsetting
        """
        for order_by_rule in order_by or []:
            if order_by_rule.taxon not in return_taxons:
                raise InvalidRequest(
                    'request.order_by', f'Taxon "{order_by_rule.taxon}" used in order_by clause must be also selected.'
                )

        projected_sql_and_df_columns, final_query = cls._project_columns(calc_df.query, calc_df, return_taxons)
        final_query = final_query.select_from(calc_df.query)

        projected_df_columns = Dataframe.dataframe_columns_to_map(
            [df_col for _, df_col in projected_sql_and_df_columns]
        )

        if order_by:
            final_query = final_query.order_by(
                *[
                    nullslast(ORDER_BY_FUNCTIONS[item.type](column(safe_identifier(item.taxon))))
                    for item in (order_by or [])
                ]
            )

        if limit is not None:
            final_query = final_query.limit(limit)
        if offset is not None:
            final_query = final_query.offset(offset)

        return Dataframe(
            final_query,
            projected_df_columns,
            calc_df.used_model_names,
            used_physical_data_sources=physical_data_sources,
        )

    @classmethod
    def _project_column(
        cls,
        query: Select,
        taxon: Taxon,
        source_df_column: Optional[DataframeColumn],
    ) -> ColumnAndDataframeColumn:
        """
        Returns projection SQL for given taxon, and also description of that projected column in a form of
        DataframeColumn.
        """
        try:
            col = query.columns[taxon.slug_safe_sql_identifier]
            assert (
                source_df_column is not None
            ), f'DataframeColumn is required for dimension types. taxon_slug: {taxon.slug}'
            if source_df_column.quantity_type == ValueQuantityType.array:
                # We want to cast array into a string when selecting it.
                # The final quantity is thus always scalar.
                col = cast(col, String)
            df_col = DataframeColumn(TaxonExpressionStr(taxon.slug), taxon, ValueQuantityType.scalar)

            return col.label(taxon.slug_safe_sql_identifier), df_col
        except TelExpressionException as error:
            raise HuskyInvalidTelException(error, taxon.slug)

    @classmethod
    def _project_columns(
        cls, query: Select, dataframe: Dataframe, return_taxons: Dict[TaxonExpressionStr, Taxon]
    ) -> Tuple[List[ColumnAndDataframeColumn], Select]:
        projected_sql_and_df_columns: List[ColumnAndDataframeColumn] = [
            cls._project_column(query, taxon, dataframe.slug_to_column.get(taxon_slug_expression))
            for taxon_slug_expression, taxon in return_taxons.items()
        ]

        return (
            projected_sql_and_df_columns,
            Select(columns=sort_columns([col for col, _ in projected_sql_and_df_columns])),
        )
