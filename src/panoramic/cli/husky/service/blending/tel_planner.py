from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple, cast

from sqlalchemy import column

from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType, TaxonTypeEnum
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingTelData,
)
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.result import PostFormula, PreFormula, TaxonToTemplate
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.filter_clauses import FilterClause
from panoramic.cli.husky.service.types.api_data_request_types import BlendingDataRequest
from panoramic.cli.husky.service.utils.exceptions import (
    HuskyInvalidTelException,
    InvalidRequest,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
)


class TelPlan:
    data_source_formula_templates: Dict[str, List[SqlFormulaTemplate]]
    comparison_data_source_formula_templates: Dict[str, List[SqlFormulaTemplate]]
    dimension_formulas: List[PreFormula]
    comparison_dimension_formulas: List[PreFormula]
    metric_pre: List[PreFormula]
    metric_post: List[Tuple[PostFormula, Taxon]]
    """
    List of formulas SQL formulas and taxons for the last phase
    """

    data_source_filter_templates: Dict[str, TaxonToTemplate]

    comparison_join_columns: List[str]
    """
    List of columns to join data and comparison dataframes
    """

    comparison_raw_taxon_slugs: List[TaxonExpressionStr]
    """
    List of raw taxon slugs to use for comparison
    """

    override_mappings: OverrideMappingTelData
    """
    List of override mappings referenced in the result
    """

    comparison_override_mappings: OverrideMappingTelData
    """
    List of override mappings referenced in the result of comparison query
    """

    def __init__(self):
        self.data_source_formula_templates = defaultdict(list)
        self.comparison_data_source_formula_templates = defaultdict(list)
        self.data_source_filter_templates = defaultdict(dict)
        self.dimension_formulas = []
        self.comparison_dimension_formulas = []
        self.metric_pre = []
        self.metric_post = []
        self.comparison_join_columns = []
        self.comparison_raw_taxon_slugs = []
        self.override_mappings = set()
        self.comparison_override_mappings = set()


class TelPlanner:
    @classmethod
    def plan(
        cls,
        ctx: HuskyQueryContext,
        request: BlendingDataRequest,
        projection_taxons: TaxonMap,
        all_taxons: TaxonMap,
        taxon_to_ds: Dict[str, Set[str]],
    ) -> TelPlan:
        """
        Prepares taxons plan
        """
        plan = TelPlan()
        result_cache = dict()
        all_data_sources = {subreq.properties.data_source for subreq in request.data_subrequests}
        for taxon in projection_taxons.values():
            if taxon.calculation:
                original_slug = taxon.comparison_taxon_slug_origin or taxon.slug
                taxon_data_sources = taxon_to_ds[original_slug]
                result = cls._parse_taxon_expr(ctx, taxon, taxon.slug, taxon_data_sources, all_taxons)
                result_cache[taxon.slug] = result

                # Create dict for dim templates, key is data source
                for ds_formula in result.data_source_formula_templates:
                    plan.data_source_formula_templates[ds_formula.data_source].append(ds_formula)

                plan.dimension_formulas.extend(result.dimension_formulas)
                plan.metric_pre.extend(result.pre_formulas)
                plan.metric_post.append((result.post_formula, taxon))

                plan.override_mappings.update(result.override_mappings)
            else:
                sql_slug = column(taxon.slug_safe_sql_identifier)
                if taxon.is_dimension:
                    aggregation = taxon.aggregation or AggregationDefinition(type=AggregationType.group_by)
                else:
                    aggregation = taxon.aggregation or AggregationDefinition(type=AggregationType.sum)

                plan.metric_pre.append(PreFormula(sql_slug, taxon.slug, aggregation))
                plan.metric_post.append((PostFormula(sql_slug), taxon))

        if request.comparison and request.comparison.taxons:
            for taxon in [all_taxons[slug] for slug in request.comparison.taxons]:
                if taxon.calculation:
                    taxon_data_sources = all_data_sources
                    result = cls._parse_taxon_expr(
                        ctx, taxon, 'comp_join_col_' + taxon.slug, taxon_data_sources, all_taxons
                    )
                    # Create dict for dim templates, key is data source
                    for ds_formula in result.data_source_formula_templates:
                        plan.data_source_formula_templates[ds_formula.data_source].append(ds_formula)

                    if result.override_mappings:
                        plan.override_mappings.update(result.override_mappings)
                        plan.comparison_override_mappings.update(result.override_mappings)

                    plan.dimension_formulas.extend(result.dimension_formulas)
                    for ds_formula in result.data_source_formula_templates:
                        plan.comparison_data_source_formula_templates[ds_formula.data_source].append(ds_formula)
                    plan.comparison_dimension_formulas.extend(result.dimension_formulas)
                    for dim_formula in result.dimension_formulas:
                        plan.comparison_join_columns.append(dim_formula.label)
                else:
                    # Raw comparison join taxon taxon.. add it to join and also to select from dataframes
                    plan.comparison_join_columns.append(taxon.slug_safe_sql_identifier)
                    plan.comparison_raw_taxon_slugs.append(taxon.slug_safe_sql_identifier)

        cls._populate_filter_templates_to_plan(ctx, plan, request, all_taxons)

        return plan

    @classmethod
    def _populate_filter_templates_to_plan(
        cls, ctx: HuskyQueryContext, plan: TelPlan, request: BlendingDataRequest, all_taxons: TaxonMap
    ):
        """
        Prepare sql templates for filters, keyed by data source and then by taxon slug.
        In general, TelPlan filtering works like this:
        1. create template for each subrequest filter taxon (raw and computed)
        2. pass that template as dict to the single husky
        3. In select builder, render these templates to create records into taxon_model_info_map,
         especially the sql accessor property.
         :param ctx:
        """
        for subrequest in request.data_subrequests:
            data_source = subrequest.properties.data_source
            filter_templates = cls.get_preaggregation_filter_templates(
                ctx,
                [subrequest.preaggregation_filters, subrequest.scope.preaggregation_filters],
                all_taxons,
                data_source,
            )
            plan.data_source_filter_templates[data_source] = filter_templates

    @classmethod
    def get_preaggregation_filter_templates(
        cls,
        ctx: HuskyQueryContext,
        filter_clauses: List[Optional[FilterClause]],
        all_taxons: TaxonMap,
        data_source: str,
    ) -> TaxonToTemplate:
        """
        Creates sql templates for each taxon. Returns them keys by taxon slug.
        """
        taxons_to_template: TaxonToTemplate = dict()
        for filter_clause in filter_clauses:
            if filter_clause:
                taxon_slugs = filter_clause.get_taxon_slugs()
                for slug in taxon_slugs:
                    taxon = all_taxons[cast(TaxonExpressionStr, slug)]
                    if not taxon.is_dimension:
                        exc = InvalidRequest(
                            'request.preaggregation_filters',
                            f'Metric taxons are not allowed in preaggregation filters. Remove filter for taxon {taxon.slug}',
                        )
                        raise exc
                    if taxon.calculation:
                        result = cls._parse_taxon_expr(
                            ctx, taxon, taxon.slug, [data_source], all_taxons, subrequest_only=True
                        )
                        taxons_to_template[taxon.slug_expr] = result.data_source_formula_templates[0]
                    else:
                        taxons_to_template[taxon.slug_expr] = SqlFormulaTemplate(
                            SqlTemplate(f'${{{taxon.slug}}}'), taxon.slug_expr, data_source, {taxon.slug_expr}
                        )
        return taxons_to_template

    @staticmethod
    def _parse_taxon_expr(
        ctx: HuskyQueryContext,
        taxon: Taxon,
        tel_prefix: str,
        data_sources: Iterable[str],
        all_taxons: TaxonMap,
        subrequest_only=False,
    ):
        taxon_type = EnumHelper.from_value(TaxonTypeEnum, taxon.taxon_type)
        try:
            return TaxonTelDialect().render(
                expr=cast(str, taxon.calculation),
                ctx=ctx,
                taxon_map=all_taxons,
                taxon_slug=tel_prefix,
                comparison=taxon.is_comparison_taxon,
                data_sources=data_sources,
                taxon_type=taxon_type,
                aggregation=taxon.aggregation,
                subrequest_only=subrequest_only,
            )
        except TelExpressionException as error:
            raise HuskyInvalidTelException(error, taxon.slug)
