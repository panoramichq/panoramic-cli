from typing import Dict, List

from sqlalchemy.sql import Select

from panoramic.cli.husky.core.sql_alchemy_util import sort_columns
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingSlug,
    OverrideMappingTelData,
)
from panoramic.cli.husky.core.tel.result import PreFormula
from panoramic.cli.husky.service.blending.features.override_mapping.sql import (
    OverrideMappingSql,
)
from panoramic.cli.husky.service.types.types import Dataframe


class DimensionPhaseBuilder:
    @classmethod
    def calculate_dataframe(
        cls,
        dimension_formulas: List[PreFormula],
        override_mappings_tel_data: OverrideMappingTelData,
        override_mapping_cte_map: Dict[OverrideMappingSlug, Select],
        df: Dataframe,
    ) -> Dataframe:
        select_columns = []
        select_columns.extend(df.query.columns)
        for dim_formula in dimension_formulas:
            col = dim_formula.formula.label(dim_formula.label)
            select_columns.append(col)

        # add joins to relevant override mapping CTEs
        select_from_query = OverrideMappingSql.insert_cte_joins(
            df.query, override_mappings_tel_data, override_mapping_cte_map
        )

        query = Select(columns=sort_columns(select_columns)).select_from(select_from_query)
        return Dataframe(query, df.slug_to_column, df.used_model_names, df.used_physical_data_sources)
