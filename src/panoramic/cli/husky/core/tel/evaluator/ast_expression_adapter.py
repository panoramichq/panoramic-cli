from typing import Any, Optional, Set, cast

from sqlalchemy import column

from panoramic.cli.husky.core.tel.evaluator.context import TelRootContext
from panoramic.cli.husky.core.tel.evaluator.expressions import TelExpression
from panoramic.cli.husky.core.tel.result import (
    ExprResult,
    PostFormula,
    UsedTaxonSlugsContainer,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


def adapt_tel_expression(expr: TelExpression, context: TelRootContext, debug_info: Optional[Any] = None) -> ExprResult:
    """
    Function producing the ExprResult from the AST node. This is the boundary between the Tel AST and the rest of the
    world.
    """
    query_result = expr.result(context)

    return ExprResult(
        pre_formulas=query_result.aggregations,
        post_formula=PostFormula(
            column(query_result.label) if query_result.label else query_result.sql,
            template=query_result.template,
            exclude_slugs=cast(Set[TaxonExpressionStr], query_result.exclude_slugs),
        ),
        dimension_formulas=query_result.dimension_formulas,
        data_source_formula_templates=query_result.data_source_formula_templates,
        phase=expr.phase(context),
        used_taxons=UsedTaxonSlugsContainer.create_from_taxons(expr.used_taxons(context)),
        invalid_value=expr.invalid_value(context),
        return_data_sources=expr.return_data_sources(context),
        return_type=expr.return_type(context),
        template_slugs=expr.template_slugs(context),
        debug_info=debug_info,
        override_mappings=query_result.override_mappings,
    )
