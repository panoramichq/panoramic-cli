from collections import defaultdict
from typing import Dict, List, Optional, Set

from sqlalchemy import column, select, text
from sqlalchemy.sql import Selectable, functions
from sqlalchemy.sql.elements import ColumnClause, TextClause, and_, literal_column, or_

from panoramic.cli.husky.core.sql_alchemy_util import (
    quote_identifier,
    safe_identifier,
    safe_identifiers_iterable,
    safe_quote_identifier,
    sort_columns,
)
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate
from panoramic.cli.husky.service.blending.tel_planner import TelPlan
from panoramic.cli.husky.service.constants import HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.types.types import Dataframe, DataframeColumn
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


def _prepare_slug_to_dataframes(dataframes: List[Dataframe]) -> Dict[TaxonExpressionStr, List[Dataframe]]:
    slug_to_dataframes: Dict[TaxonExpressionStr, List[Dataframe]] = defaultdict(list)
    for df in dataframes:
        for slug in df.slug_to_column.keys():
            slug_to_dataframes[slug].append(df)
    return slug_to_dataframes


def blend_dataframes(
    ctx: HuskyQueryContext,
    dataframes: List[Dataframe],
    data_source_formula_templates: Optional[Dict[str, List[SqlFormulaTemplate]]] = None,
) -> Dataframe:
    """
    Produces new blended dataframe from all the given dataframes joined on all dimensions that appear at least twice in
    different dataframes.
    """
    slug_to_dataframes: Dict[TaxonExpressionStr, List[Dataframe]] = _prepare_slug_to_dataframes(dataframes)
    dataframe_to_query: Dict[Dataframe, Selectable] = dict()
    used_model_names: Set[str] = set()
    used_physical_sources: Set[str] = set()
    for idx, df in enumerate(dataframes):
        # Create query for each dataframe, that has alias as 'q<number>'
        dataframe_to_query[df] = df.query.alias(f'q{idx}')
        used_model_names.update(df.used_model_names)
        used_physical_sources.update(df.used_physical_data_sources)

    selectors: List[TextClause] = []
    dimension_columns: List[ColumnClause] = []
    # Prepare list of sql selectors. If it is a metric, do zeroifnull(q0.metric + q1.metric + ...)
    # If it is a dimension, just select it. Because we are using USING clause, no need for coalesce.
    for taxon_slug in sorted(slug_to_dataframes.keys()):
        dataframes_with_slug = slug_to_dataframes[taxon_slug]
        taxon = dataframes_with_slug[0].slug_to_column[taxon_slug].taxon
        taxon_column = quote_identifier(taxon.slug_safe_sql_identifier, ctx.dialect)
        query_aliases = [dataframe_to_query[df].name for df in dataframes_with_slug]
        if taxon.is_dimension:
            if len(query_aliases) > 1:
                # Coalesce must have two or more args
                dimension_coalesce = functions.coalesce(
                    *[literal_column(f'{query_alias}.{taxon_column}') for query_alias in query_aliases]
                )
            else:
                #  No need to coalesce now
                dimension_coalesce = literal_column(f'{query_aliases[0]}.{taxon_column}')
            col = dimension_coalesce.label(taxon.slug_safe_sql_identifier)

            dimension_columns.append(col)
            selectors.append(col)
        else:
            if taxon.data_source:
                # do not use coalesce aka zeroifnull when summing namespaces taxons..
                # There are using TEL expr, where null is handled by TEL compilation.
                summed = '+'.join([f'{query_alias}.{taxon_column}' for query_alias in query_aliases])
            else:
                summed = '+'.join([f'coalesce({query_alias}.{taxon_column},0)' for query_alias in query_aliases])
            selectors.append(text(f'sum({summed}) as {taxon_column}'))

    final_columns: List[ColumnClause] = []
    if data_source_formula_templates:
        for pre_formulas in data_source_formula_templates.values():
            for pre_formula in pre_formulas:
                col = column(pre_formula.label)
                dimension_columns.append(col)
                selectors.append(col)
                final_columns.append(column(quote_identifier(pre_formula.label, ctx.dialect)))

    # All taxons in final DF
    final_slug_to_taxon: Dict[TaxonExpressionStr, DataframeColumn] = dataframes[0].slug_to_column.copy()

    # Because of sql alchemy compiler putting extra () around every using select_from, we first join all queries
    # And then define the aggregation selectors (right after this for loop)
    join_query = dataframe_to_query[dataframes[0]]
    for i in range(1, len(dataframes)):
        #  Iterate dataframes, and do full outer join on FALSE, effectively meaning UNION-ALL without the need to
        # align all columns
        dataframe_to_join = dataframes[i]
        used_physical_sources.update(dataframe_to_join.used_physical_data_sources)

        final_slug_to_taxon = {**final_slug_to_taxon, **dataframe_to_join.slug_to_column}
        join_from = join_query
        join_to = dataframe_to_query[dataframe_to_join]

        # On purpose joining on value that will always return FALSE => PROD-8136
        join_query = join_from.join(
            join_to,
            dataframe_to_query[dataframes[0]].columns[HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME]
            == join_to.columns[HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME],
            full=True,
        )

    aggregate_join_query = select(selectors).select_from(join_query)
    for dimension_column in dimension_columns:
        aggregate_join_query = aggregate_join_query.group_by(dimension_column)

    # We have to wrap it in one more select, so the alchemy query object has columns referencable via 'c' attribute.
    final_columns.extend(column(id_) for id_ in safe_identifiers_iterable(final_slug_to_taxon.keys()))
    query = select(sort_columns(final_columns)).select_from(aggregate_join_query)

    return Dataframe(query, final_slug_to_taxon, used_model_names, used_physical_sources)


def left_join_dataframes(
    ctx: HuskyQueryContext, data_dataframe: Dataframe, comparison_dataframe: Dataframe, tel_plan: TelPlan
) -> Dataframe:
    """
    Produces new DF, that is DATA_DF LEFT JOIN COMPARISON_DF on given list of taxons.
    :param ctx: Husky query context
    :param data_dataframe: df to left join to
    :param comparison_dataframe: other df
    :param tel_plan: Current TEL plan
    :return: Left joined dataframe
    """
    # Alias their queries to be able to easily reference them.
    data_table = data_dataframe.query.alias('data_dataframe')
    comparison_table = comparison_dataframe.query.alias('comparison_dataframe')

    # Union taxon slugs from both DFs.
    columns_by_slug = {**data_dataframe.slug_to_column, **comparison_dataframe.slug_to_column}
    select_columns = set()
    #  Select the column from specific data frame (data or comparison), but then label them to remove that prefix,
    # since the names are already unique (from TEL planner)
    for slug in data_dataframe.slug_to_column.keys():
        select_columns.add(
            literal_column(f'data_dataframe.{safe_quote_identifier(slug, ctx.dialect)}').label(safe_identifier(slug))
        )

    for slug, df_column in comparison_dataframe.slug_to_column.items():
        taxon: Taxon = df_column.taxon
        if taxon.is_comparison_taxon:
            select_columns.add(
                literal_column(f'comparison_dataframe.{safe_quote_identifier(slug, ctx.dialect)}').label(
                    safe_identifier(slug)
                )
            )
    join_on_conditions = []

    for template in tel_plan.dimension_formulas:
        # Select the data source formula labels explicitly from data table
        select_columns.add(data_table.c[template.label])

    for join_column in tel_plan.comparison_join_columns:
        join_on_conditions.append(
            # Account for dimensions that can have NULL values, because NULL = NULL evaluates to FALSE in SQL,
            # second condition that compares both columns to IS NULL needs to be added.
            or_(
                data_table.c[join_column] == comparison_table.c[join_column],
                and_(data_table.c[join_column].is_(None), comparison_table.c[join_column].is_(None)).self_group(),
            )
        )

    if len(join_on_conditions) == 0:
        # In case there were no comparison dimensions defined, the comparison dataframe also has no dimensions
        # (thus it is one row) and we can safely do a join without ON clause to data dataframe.
        # Using 1=1 as a easiest way to do join without ON clause in alchemy...
        join_on_conditions.append(text('1=1'))

    q = select(sort_columns(list(select_columns))).select_from(
        data_table.outerjoin(comparison_table, and_(*join_on_conditions))
    )

    return Dataframe(
        q,
        columns_by_slug,
        data_dataframe.used_model_names | comparison_dataframe.used_model_names,
        data_dataframe.used_physical_data_sources | comparison_dataframe.used_physical_data_sources,
    )
