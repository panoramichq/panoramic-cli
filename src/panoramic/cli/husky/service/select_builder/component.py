from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, cast

from sqlalchemy import func, nullslast, select
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnClause, TextClause, literal_column

from panoramic.cli.husky.common.sqlalchemy_ext import my_sql_text
from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.sql_alchemy_util import (
    compile_query,
    safe_identifier,
    safe_quote_identifier,
    sort_columns,
)
from panoramic.cli.husky.core.taxonomy.aggregations import (
    AggregationParamsSortDimension,
)
from panoramic.cli.husky.core.taxonomy.enums import (
    ORDER_BY_FUNCTIONS,
    AggregationType,
    TaxonOrderType,
)
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.result import TaxonToTemplate
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.component import FilterBuilder
from panoramic.cli.husky.service.filter_builder.filter_clauses import FilterClause
from panoramic.cli.husky.service.graph_builder.component import Graph
from panoramic.cli.husky.service.select_builder.graph_search import GraphSearch
from panoramic.cli.husky.service.select_builder.query_joins import (
    QueryJoins,
    SimpleQueryJoins,
    get_bfs_ordered,
)
from panoramic.cli.husky.service.select_builder.scope_guard import ScopeGuard
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.types.api_scope_types import Scope
from panoramic.cli.husky.service.types.types import EffectivelyUsedModels
from panoramic.cli.husky.service.utils.exceptions import ModelNotFoundException
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    SlugExprTaxonMap,
    TaxonSlugExpression,
)


class SelectBuilder:

    _AGGREGATION_WINDOW_FUNCTIONS: Dict[AggregationType, func.Function] = {
        AggregationType.first_by: func.FIRST_VALUE,
        AggregationType.last_by: func.LAST_VALUE,
    }
    """Map of aggregation window functions"""

    def __init__(
        self,
        ctx: HuskyQueryContext,
        scope: Scope,
        graph_select_taxons: SlugExprTaxonMap,
        projection_taxons: SlugExprTaxonMap,
        graph: Graph,
        data_source: str,
        filter_clause: Optional[FilterClause] = None,
        dimension_templates: Optional[List[SqlFormulaTemplate]] = None,
        filter_templates: Optional[TaxonToTemplate] = None,
    ):
        """
        Builds the core select query.
        :param graph_select_taxons Taxons to find in a graph (used in projection or pre agg filter)
        :param projection_taxons Taxons to SELECT
        """
        self.ctx = ctx
        self.scope: Scope = scope
        self.graph_select_taxons: SlugExprTaxonMap = graph_select_taxons
        self.projection_taxons: SlugExprTaxonMap = projection_taxons
        self.graph = graph
        self.filter_clause = filter_clause
        self.taxon_model_info_map: Dict[str, TaxonModelInfo] = dict()
        self.dimension_templates: List[SqlFormulaTemplate] = dimension_templates or []
        self.filter_templates: TaxonToTemplate = filter_templates or {}
        self.data_source: str = data_source

    def _find_noncached_query_joins(self) -> SimpleQueryJoins:
        """
        Finds query join tree, not using cached models.
        """
        return GraphSearch(
            self.graph.name_to_model, set(self.graph_select_taxons.keys()), self.graph.model_graph, self.data_source
        ).find_join_tree()

    def _window_aggregation_query_required(self) -> bool:
        """
        Returns whether the provided list of raw taxons requires aggregations using window functions
        """
        return any(
            [
                taxon.aggregation and taxon.aggregation.type in self._AGGREGATION_WINDOW_FUNCTIONS
                for taxon in self.graph_select_taxons.values()
            ]
        )

    def get_query(self) -> Tuple[Select, Dict[str, TaxonModelInfo], EffectivelyUsedModels]:
        """
        Builds the core select query.
        """
        noncached_query_joins = self._find_noncached_query_joins()
        ordered_query_joins = get_bfs_ordered(noncached_query_joins)
        # note list of noncached models used in the query (since no viable cached models were found)
        effectively_used_models = {'noncached_models': [simple_join.model.name for simple_join in ordered_query_joins]}

        # Match models to taxons
        taxon_to_model = SelectBuilder._build_taxon_to_model_dict(ordered_query_joins)

        self._build_taxon_model_info_map(self.graph_select_taxons, taxon_to_model)

        if self._window_aggregation_query_required():
            # there are raw taxons which require aggregation using window function
            query = self._build_query_window_aggregations(taxon_to_model, ordered_query_joins)
        else:
            query = self._build_query_distribution(taxon_to_model, ordered_query_joins)

        if self.filter_clause:
            query = FilterBuilder.augment_query(self.ctx, query, self.taxon_model_info_map, self.filter_clause)

        return query, self.taxon_model_info_map, EffectivelyUsedModels(effectively_used_models)

    def _rebuild_taxon_info_map_inner_query(self):
        """
        Updates the internal taxon model info map, because we use inner query to select the raw data
        """
        taxon_model_info_map = dict()
        for taxon_slug_expression, info in self.taxon_model_info_map.items():
            new_info = TaxonModelInfo(safe_identifier(taxon_slug_expression), info.model_name, info.quantity_type)
            taxon_model_info_map[taxon_slug_expression] = new_info

        for filter_slug, formula in self.filter_templates.items():
            if filter_slug in taxon_model_info_map:
                info = taxon_model_info_map[filter_slug]
                render_params = dict()
                for used_slug in formula.used_taxons:
                    render_params[used_slug] = taxon_model_info_map[used_slug].taxon_sql_accessor
                sql_accessor = formula.render_formula(**render_params)

                taxon_model_info_map[filter_slug] = TaxonModelInfo(sql_accessor, info.model_name, info.quantity_type)

        self.taxon_model_info_map = taxon_model_info_map

    def _build_query_window_aggregations(
        self,
        taxon_to_model: Dict[TaxonSlugExpression, HuskyModel],
        ordered_query_joins: Sequence[QueryJoins],
    ) -> Select:
        """
        Generates query for taxons which need window functions for aggregation

        :param taxon_to_model: Map of taxon slugs (key) and models they are coming from (value)
        :param ordered_query_joins: List of joins
        """
        selectors = []
        # generate inner query with window aggregation functions
        for taxon_slug_expression, taxon in sorted(self.projection_taxons.items(), key=lambda x: str(x[0])):
            model = taxon_to_model[taxon_slug_expression]
            if (
                taxon.tel_metadata
                and taxon.tel_metadata.aggregation_definition
                and taxon.tel_metadata.aggregation_definition.params
                and taxon.tel_metadata_aggregation_type in self._AGGREGATION_WINDOW_FUNCTIONS
            ):
                # find the order_by columns
                order_by = []
                window_params = cast(AggregationParamsSortDimension, taxon.tel_metadata.aggregation_definition.params)
                for field in window_params.sort_dimensions:
                    col = taxon_to_model[TaxonSlugExpression(field.taxon)].taxon_sql_accessor(self.ctx, field.taxon)

                    order_by_dir = field.order_by or TaxonOrderType.asc
                    order_by.append(nullslast(ORDER_BY_FUNCTIONS[order_by_dir](literal_column(col))))

                # apply window aggregation functions
                column = self._AGGREGATION_WINDOW_FUNCTIONS[taxon.tel_metadata_aggregation_type](
                    literal_column(model.taxon_sql_accessor(self.ctx, taxon.slug))
                ).over(partition_by=self.get_partition_by_columns(model), order_by=order_by)
            else:
                # otherwise, render the columns "as-is"
                column = literal_column(model.taxon_sql_accessor(self.ctx, taxon.slug))

            selectors.append(column.label(taxon.slug_safe_sql_identifier))

        # add joins to the inner query
        inner_query = select(selectors).select_from(self._build_from_joins(ordered_query_joins))

        # apply scope filters to the inner query
        inner_query = ScopeGuard.add_scope_row_filters(self.ctx, self.scope, inner_query, self.taxon_model_info_map)

        # update taxon model info map, because we're selecting from outer query and not the inner query
        self._rebuild_taxon_info_map_inner_query()

        # then, we use prepare the outer query on which we can safely apply GROUP BY
        return self._build_selectors(lambda _, taxon_slug: safe_identifier(taxon_slug)).select_from(inner_query)

    def _build_query_distribution(
        self,
        taxon_to_model: Dict[TaxonSlugExpression, HuskyModel],
        ordered_query_joins: Sequence[QueryJoins],
    ) -> Select:
        """
        Generates query for models with no window aggregation functions

        :param taxon_to_model: Map of taxon slugs (key) and models they are coming from (value)
        :param ordered_query_joins: List of joins
        """
        query = self._build_selectors(
            lambda taxon_slug_expression, taxon_slug: taxon_to_model[taxon_slug_expression].taxon_sql_accessor(
                self.ctx, taxon_slug
            ),
        )
        query = query.select_from(self._build_from_joins(ordered_query_joins))

        query = ScopeGuard.add_scope_row_filters(self.ctx, self.scope, query, self.taxon_model_info_map)
        return query

    def get_partition_by_columns(self, model: HuskyModel):
        return [
            literal_column(model.taxon_sql_accessor(self.ctx, model_attribute.taxon))
            for model_attribute in model.attributes_memoized.values()
            if model_attribute.identifier is True
        ]

    @staticmethod
    def _build_taxon_to_model_dict(ordered_query_joins: Sequence[QueryJoins]) -> Dict[TaxonSlugExpression, HuskyModel]:
        """
        Dict mapping taxon slug to model it should be selected from
        """
        taxon_to_model = dict()

        already_defined_taxon: Set[TaxonSlugExpression] = set()
        for join in ordered_query_joins:
            for taxon_slug in join.taxons_from_model:
                if taxon_slug in already_defined_taxon:
                    continue
                already_defined_taxon.add(taxon_slug)
                if join.model:
                    taxon_to_model[taxon_slug] = join.model

        return taxon_to_model

    def _build_taxon_model_info_map(
        self, taxons: Dict[TaxonSlugExpression, Taxon], taxon_to_model: Dict[TaxonSlugExpression, HuskyModel]
    ):
        """
        Extract extra information (currently only if scalar/array) about taxons on models
        Currently it's hardcoded to mark all *_tags taxons as taxons of type array
        """
        taxon_model_info_map = dict()

        for taxon_slug_expression in taxons:
            model = taxon_to_model[taxon_slug_expression]
            taxon_column_selector = self._get_column_accessor_for_taxon_and_model(model, taxon_slug_expression)
            info = TaxonModelInfo(
                compile_query(taxon_column_selector, self.ctx.dialect),
                model.name,
                model.get_attribute_by_taxon(taxon_slug_expression.slug).quantity_type,
            )

            taxon_model_info_map[taxon_slug_expression.slug] = info
        for filter_slug, template in self.filter_templates.items():
            if filter_slug not in taxon_model_info_map:
                # If the slug is in the info map, it means it is raw slug, and we dont need to create
                # sql accessor for it
                render_params = dict()
                for used_slug in template.used_taxons:
                    render_params[used_slug] = taxon_model_info_map[used_slug].taxon_sql_accessor
                sql_accessor = template.render_formula(**render_params)
                taxon_model_info_map[filter_slug] = TaxonModelInfo(sql_accessor, None, None)

        self.taxon_model_info_map = taxon_model_info_map

    def _get_column_accessor_for_taxon_and_model(
        self, model: HuskyModel, taxon_slug_expression: TaxonSlugExpression
    ) -> ColumnClause:
        return literal_column(model.taxon_sql_accessor(self.ctx, taxon_slug_expression.slug))

    def _build_selectors(
        self,
        get_column_name: Callable[[TaxonSlugExpression, str], str],
    ) -> Select:
        """
        Returns the select part of query.
        """
        selectors = []

        for taxon_slug_expression, taxon in self.projection_taxons.items():
            column_name = get_column_name(taxon_slug_expression, taxon_slug_expression.slug)
            col = literal_column(column_name)

            selectors.append(col.label(taxon.slug_safe_sql_identifier))

        for template in self.dimension_templates:
            # We must render the dimension templates with correct sql columns
            slug_to_column = {slug: get_column_name(TaxonSlugExpression(slug), slug) for slug in template.used_taxons}
            sql_formula = template.render_formula(**slug_to_column)
            col = literal_column(sql_formula).label(template.label)
            selectors.append(col)
        return select(sort_columns(selectors))

    def _build_from_joins(self, ordered_query_joins: Sequence[QueryJoins]) -> TextClause:
        """
        Returns the from and join statements.
        """
        # Adding parts of sql clauses to this array, that is at the end joined on ' '.
        if not ordered_query_joins[0].model:
            raise ModelNotFoundException()

        root_model: HuskyModel = ordered_query_joins[0].model

        # generate FROM clause for root model (support table alias)
        unique_root_model_name = root_model.full_object_name(self.ctx)
        if root_model.table_alias is None:
            safe_name = safe_quote_identifier(root_model.full_object_name(self.ctx), self.ctx.dialect)
            unique_root_model_name += f' AS {safe_name}'
        else:
            unique_root_model_name += f' AS {root_model.table_alias}'

        join_query = [unique_root_model_name]  # Table for the FROM clause.

        bind_params: Dict[str, Any] = {}
        for join_from in ordered_query_joins:
            # generate JOIN clauses for the query join
            join_query.append(join_from.to_sql(self.ctx))
            # get bind parameters (if there are any) so we can bind them later on
            bind_params.update(join_from.bind_params())

        query_str = ' '.join(join_query)
        return my_sql_text(query_str).bindparams(**bind_params)
