from typing import Union

from antlr4.tree.Tree import TerminalNodeImpl

from panoramic.cli.husky.core.federated.model.tel.data_structures import (
    AttributeValidationTelVisitorParams,
    AttributeValidationTelVisitorResult,
)
from panoramic.cli.husky.core.federated.model.tel.exceptions import (
    ModelTelCyclicReferenceException,
    ModelTelExpressionException,
)
from panoramic.cli.husky.core.tel.helper import TelVisitorHelper
from panoramic.cli.husky.core.tel.tel_dialect import ModelTelDialect
from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser
from panoramic.cli.tel_grammar.TelVisitor import TelVisitor as AntlrTelVisitor


class AttributeValidationTelVisitor(AntlrTelVisitor):
    """
    TEL visitor for model attribute transformations. It should be used to validate TEL expression
    """

    def __init__(self, parameters: AttributeValidationTelVisitorParams):
        self._result = AttributeValidationTelVisitorResult()
        self._parameters = parameters

    @property
    def result(self) -> AttributeValidationTelVisitorResult:
        """
        Result from this visitor
        """
        return self._result

    def _create_empty_duplicate(self) -> 'AttributeValidationTelVisitor':
        """
        Creates empty (no result) duplicate of this visitor:
        - same parameters
        - remembers used taxons/column names
        """
        tel_visitor = AttributeValidationTelVisitor(self._parameters)
        tel_visitor._result.append_with(self._result)
        tel_visitor._result.reset_result_expression()

        return tel_visitor

    @classmethod
    def _throw_with_details(cls, msg: str, ctx):
        if isinstance(ctx, TerminalNodeImpl):
            details = TelVisitorHelper.get_exception_details_from_node(ctx)
        else:
            details = TelVisitorHelper.get_exception_details_from_ctx(ctx)
        raise ModelTelExpressionException.create_with_details(
            msg, position=details.position, line=details.line, expression=details.expression
        )

    def _visit_nonempty_argument(self, ctx) -> 'AttributeValidationTelVisitor':
        visitor = self._create_empty_duplicate()
        visitor.visit(ctx)

        result_expression = visitor.result.result_expression
        if len(result_expression) == 0 or result_expression == ',':
            self._throw_with_details('Invalid expression', ctx)

        return visitor

    def visitFn(self, ctx: AntlrTelParser.FnContext):
        """
        Handles rendering TEL functions
        """
        fn_name = str(ctx.children[0])
        child_ctxs = TelVisitorHelper.get_fn_arg_contexts(ctx)

        self._result.add_to_expressions(f'{fn_name}(')
        for idx, child_ctx in enumerate(child_ctxs):
            if idx > 0:
                self._result.add_to_expressions(', ')

            child_visitor = self._visit_nonempty_argument(child_ctx)
            self._result.append_with(child_visitor.result)

        self._result.add_to_expressions(')')

    def visitTaxon_expr(self, ctx: AntlrTelParser.Taxon_exprContext):
        """
        Handle taxon slugs from this model. We perform additional checks when upserting the model to ensure that
        it contains transformations without circular dependencies and all taxons are present in the model
        """
        raw_taxon_slug = TelVisitorHelper.use_terminal_visitor(ctx.children[0])

        # check for cyclic reference
        if raw_taxon_slug == self._parameters.source_taxon_slug or raw_taxon_slug in self._result.used_taxon_slugs:
            raise ModelTelCyclicReferenceException(raw_taxon_slug)

        # mark this taxon slug as used
        self._result.used_taxon_slugs.add(raw_taxon_slug)

        # taxon slug are written without namespace so we need to add it there based on model's virtual data source
        taxon_slug = raw_taxon_slug

        # recursively render transformation for this taxon from this model
        # try to fetch definition of attribute for this taxon
        attrs = [attr for attr in self._parameters.source_model_attributes if taxon_slug in attr.field_map]
        if len(attrs) == 0:
            # we dont care, if we dont find the taxon - this check is performed somewhere else
            self._result.add_to_expressions(taxon_slug)
        else:
            # otherwise, recursively parse the taxon
            model_attr = attrs[0]
            tel_visitor = self._create_empty_duplicate()
            # recursively parse TEL expression to detect cyclic references
            parsed_expression = ModelTelDialect.parse(model_attr.data_reference)
            tel_visitor.visit(parsed_expression)

            # merge the results
            self._result.append_with(tel_visitor._result)

    # Visit a parse tree produced by TelParser#multiplicationExpr.
    def visitMultiplicationExpr(self, ctx: AntlrTelParser.MultiplicationExprContext):
        """
        Handle simple */ operation in TEL
        """
        self._render_simple_arithmetic(ctx)

    def _render_simple_arithmetic(
        self, ctx: Union[AntlrTelParser.AdditiveExprContext, AntlrTelParser.MultiplicationExprContext]
    ):
        operator = str(ctx.children[1])
        arg_1_visitor = self._visit_nonempty_argument(ctx.children[0])
        arg_2_visitor = self._visit_nonempty_argument(ctx.children[2])

        self._result.append_with(arg_1_visitor.result)
        self._result.add_to_expressions(operator)
        self._result.append_with(arg_2_visitor.result)

    def visitAdditiveExpr(self, ctx: AntlrTelParser.AdditiveExprContext):
        """
        Handle simple +- operation in TEL
        """
        self._render_simple_arithmetic(ctx)

    # Visit a parse tree produced by TelParser#stringConstantAtom.
    def visitStringConstantAtom(self, ctx: AntlrTelParser.StringConstantAtomContext):
        """
        Handle column names correctly
        """
        col_name = TelVisitorHelper.use_terminal_visitor(ctx)[1:-1]
        self._result.used_column_names.add(col_name)
        self._result.add_to_expressions(f'"{col_name}"')

    def visitTerminal(self, node):
        """
        Handle running into terminal node correctly
        """
        if node.symbol.type != AntlrTelParser.EOF:
            self._result.add_to_expressions(str(node))

    def visitErrorNode(self, node):
        """
        Catch error node and return the proper error message
        """
        wrong_symbol = str(node.symbol.text)
        message = f'Unexpected symbol "{wrong_symbol}"'
        details = TelVisitorHelper.get_exception_details_from_node(node)
        raise ModelTelExpressionException.create_with_details(
            message, details.position, details.line, details.expression
        )

    def visitLogicalExpr(self, ctx: AntlrTelParser.LogicalExprContext):
        operator = str(ctx.children[1])

        arg_visitor_1 = self._visit_nonempty_argument(ctx.children[0])
        arg_visitor_2 = self._visit_nonempty_argument(ctx.children[2])

        self._result.append_with(arg_visitor_1.result)
        self._result.add_to_expressions(f' {operator} ')
        self._result.append_with(arg_visitor_2.result)
