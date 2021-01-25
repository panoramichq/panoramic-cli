from abc import ABC, abstractmethod
from typing import Generic, Iterable, Optional, Type, TypeVar

from antlr4 import CommonTokenStream, InputStream
from sqlalchemy.sql import ClauseElement

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import TaxonTypeEnum
from panoramic.cli.husky.core.tel.enums import TelDialectType
from panoramic.cli.husky.core.tel.evaluator.ast_expression_adapter import (
    adapt_tel_expression,
)
from panoramic.cli.husky.core.tel.evaluator.context import (
    TelValidationContext,
    node_id_maker,
)
from panoramic.cli.husky.core.tel.evaluator.expressions import (
    TelOptionalTaxon,
    TelRequiredTaxon,
    TelString,
)
from panoramic.cli.husky.core.tel.evaluator.model_expressions import (
    TelColumn,
    TelSQLTaxon,
)
from panoramic.cli.husky.core.tel.evaluator.visitor import (
    TelErrorListener,
    TelExpression,
    TelRootContext,
    TelVisitor,
)
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.result import ExprResult
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonMap
from panoramic.cli.tel_grammar.TelLexer import TelLexer as AntlrTelLexer
from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser

T = TypeVar('T')


MAX_TAXON_REFERENCE_DEPTH = 10


class TelDialect(ABC, Generic[T]):
    _type: TelDialectType
    double_quoted_string_ctor: Type[TelExpression]
    required_taxon_ctor: Type[TelExpression]
    optional_taxon_ctor: Type[TelExpression]

    @abstractmethod
    def render(
        self,
        expr: str,
        ctx: HuskyQueryContext,
        taxon_map: TaxonMap,
        taxon_slug: str = '',
        comparison: bool = False,
        data_sources: Optional[Iterable[str]] = None,
        taxon_type: TaxonTypeEnum = TaxonTypeEnum.metric,
        aggregation: Optional[AggregationDefinition] = None,
        subrequest_only: bool = False,
        **kwargs,
    ) -> T:
        pass

    @staticmethod
    def parse(inp: str) -> AntlrTelParser.ParseContext:
        """
        Connects Antlr classes that parse the expr and runs our TelVisitor on the AST, with the options provided.

        :param inp: TEL expression
        """
        instr = InputStream(inp)
        lexer = AntlrTelLexer(instr)
        stream = CommonTokenStream(lexer)
        parser = AntlrTelParser(stream)
        errorListener = TelErrorListener(inp)

        lexer.removeErrorListeners()
        lexer.addErrorListener(errorListener)
        parser.removeErrorListeners()
        parser.addErrorListener(errorListener)

        return parser.parse()

    @classmethod
    def visit(
        cls, inp: str, context: TelRootContext, skip_root_node: bool = False, force_optional_taxon: bool = False
    ) -> TelExpression:
        """
        Connects Antlr classes that parse the expr and runs our TelVisitor on the AST, with the options provided.

        :param inp: TEL expression
        :param context: Current root context
        :param skip_root_node: Skips creating root node
        :param force_optional_taxon: Forces all required taxons to be optional
        """

        tree = cls.parse(inp)
        if context.depth + 1 >= MAX_TAXON_REFERENCE_DEPTH:
            msg = f'Reached maximum depth of taxon references ({MAX_TAXON_REFERENCE_DEPTH}).'
            raise TelExpressionException(msg)
        visitor = TelVisitor(context, cls.visit, skip_root_node, force_optional_taxon)
        return visitor.visit(tree)

    def setup_tel_contexts(
        self,
        husky_ctx,
        taxon_map,
        aggregation=None,
        comparison=False,
        data_sources=None,
        subrequest_only=False,
        taxon_slug='',
        taxon_type=TaxonTypeEnum.metric,
    ):
        root_context = TelRootContext(
            husky_context=husky_ctx,
            tel_dialect=self,
            allowed_data_sources=data_sources,
            taxon_map=taxon_map,
            next_node_id=node_id_maker(),
            is_benchmark=comparison,
            taxon_type=taxon_type,
            taxon_slug=taxon_slug,
            aggregation=aggregation,
            subrequest_only=subrequest_only,
        )
        validation_context = TelValidationContext(root_context)
        root_context.validation_context = validation_context
        return root_context, validation_context

    @property
    def type(self):
        return self._type


class TaxonTelDialect(TelDialect[ExprResult]):
    _type = TelDialectType.TAXON
    double_quoted_string_ctor = TelString
    required_taxon_ctor = TelRequiredTaxon
    optional_taxon_ctor = TelOptionalTaxon

    def render(
        self,
        expr: str,
        ctx: HuskyQueryContext,
        taxon_map: TaxonMap,
        taxon_slug: str = '',
        comparison: bool = False,
        data_sources: Optional[Iterable[str]] = None,
        taxon_type: TaxonTypeEnum = TaxonTypeEnum.metric,
        aggregation: Optional[AggregationDefinition] = None,
        subrequest_only: bool = False,
        **kwargs,
    ) -> ExprResult:
        """
        Renders full given expression, including referenced computed taxons, recursively.
        :param expr: TEL expression to render to SQL.
        :param taxon_map: Cached taxon map that must contain all taxons used
        :param taxon_slug: Taxon slug we are generating the query for. Used for naming temporal and final SQL columns.
        :param taxon_type. Type of the taxon we are generating TEL for.
        :param comparison: True if rendering comparison taxon
        :param aggregation: Definition of aggregation function for the taxon
        """

        root_context, validation_context = self.setup_tel_contexts(
            ctx, taxon_map, aggregation, comparison, data_sources, subrequest_only, taxon_slug, taxon_type
        )

        tel_expression: TelExpression = self.visit(expr, root_context)

        tel_expression.validate(validation_context)

        validation_context.raise_for_errors()

        debug = True if 'debug' in kwargs else False

        debug_info = {}
        if debug:
            debug_info['raw_ast'] = self._debug_info_for_phase(
                tel_expression,
                root_context,
                'Phase 1, valid, raw AST - initial AST, before any optimizations, but after semantic validation.',
            )

        optimized_expression = tel_expression.rewrite(lambda e: e, root_context)

        if debug:
            debug_info['optimized_ast'] = self._debug_info_for_phase(
                optimized_expression, root_context, 'Phase 2, optimized AST, after processing optimizations.'
            )

        planned_expression = optimized_expression.plan_phase_transitions(root_context)

        if debug:
            debug_info['final_ast'] = self._debug_info_for_phase(
                planned_expression, root_context, 'Phase 3, final AST, after planning phase transitions.'
            )

        return adapt_tel_expression(planned_expression, root_context, debug_info)

    @staticmethod
    def _debug_info_for_phase(tel_expression: TelExpression, context: TelRootContext, desc: str) -> str:
        return f"""
/* {desc} */
digraph raw_ast {{
    {tel_expression.to_graphviz(context)}
}}
                """.strip().replace(
            '\n', ' '
        )


class ModelTelDialect(TelDialect[ClauseElement]):
    _type = TelDialectType.MODEL
    double_quoted_string_ctor = TelColumn
    required_taxon_ctor = TelSQLTaxon
    optional_taxon_ctor = TelSQLTaxon

    def __init__(self, unique_object_name: str, virtual_data_source: str, model):
        self._unique_object_name = unique_object_name
        self._virtual_data_source = virtual_data_source
        self._model = model

    def render(
        self,
        expr: str,
        ctx: HuskyQueryContext,
        taxon_map: TaxonMap,
        taxon_slug: str = '',
        comparison: bool = False,
        data_sources: Optional[Iterable[str]] = None,
        taxon_type: TaxonTypeEnum = TaxonTypeEnum.metric,
        aggregation: Optional[AggregationDefinition] = None,
        subrequest_only: bool = False,
        **kwargs,
    ) -> ExprResult:
        """
        Renders full given expression, including referenced computed taxons, recursively.
        :param expr: TEL expression to render to SQL.
        :param taxon_map: Cached taxon map that must contain all taxons used
        :param taxon_slug: Taxon slug we are generating the query for. Used for naming temporal and final SQL columns.
        :param taxon_type. Type of the taxon we are generating TEL for.
        :param comparison: True if rendering comparison taxon
        :param aggregation: Definition of aggregation function for the taxon
        """

        root_context, validation_context = self.setup_tel_contexts(
            ctx, taxon_map, aggregation, comparison, data_sources, subrequest_only, taxon_slug, taxon_type
        )

        tel_expression: TelExpression = self.visit(expr, root_context)

        # Validation is not currently enabled, due to lack of knowledge about data types of columns.
        # tel_expression.validate(validation_context)
        # validation_context.raise_for_errors()

        optimized_expression = tel_expression.rewrite(lambda e: e, root_context)

        return adapt_tel_expression(optimized_expression, root_context, debug_info=None)

    @property
    def unique_object_name(self) -> str:
        return self._unique_object_name

    @property
    def virtual_data_source(self) -> str:
        return self._virtual_data_source

    @property
    def model(self):
        return self._model


# class FilterTelDialect(TelDialect[FilterClause]):
#     pass
