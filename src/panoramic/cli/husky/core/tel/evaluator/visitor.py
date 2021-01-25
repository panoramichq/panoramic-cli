from typing import Callable, Dict, cast

from antlr4 import ParserRuleContext, RecognitionException, Recognizer, Token
from antlr4.error.ErrorListener import ErrorListener

from panoramic.cli.husky.core.tel.evaluator.ast import TelExpression, TelRoot
from panoramic.cli.husky.core.tel.evaluator.context import (
    ParserLocation,
    TelRootContext,
)
from panoramic.cli.husky.core.tel.evaluator.expressions import (
    TelAddition,
    TelBoolean,
    TelDivision,
    TelFloat,
    TelInteger,
    TelIsNotNull,
    TelIsNull,
    TelLogicalOperation,
    TelMultiplication,
    TelNot,
    TelParentheses,
    TelString,
    TelSubtraction,
)
from panoramic.cli.husky.core.tel.evaluator.functions import TEL_FUNCTIONS
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr
from panoramic.cli.tel_grammar.TelParser import TelParser
from panoramic.cli.tel_grammar.TelVisitor import TelVisitor as AntlrTelVisitor


class TelVisitor(AntlrTelVisitor):
    _TEL_OPERATOR_SQL_MAPPING: Dict[int, str] = {TelParser.EQ: '=', TelParser.OR: 'OR', TelParser.AND: 'AND'}

    def __init__(
        self,
        context: TelRootContext,
        parse: Callable[[str, TelRootContext, bool, bool], TelExpression],
        skip_root_node: bool = False,
        force_optional_taxon: bool = False,
    ):
        """
        Constructor

        :param context: Current root context
        :param parse: Function which starts parsing TEL expression
        :param skip_root_node: Skips creating root node
        :param force_optional_taxon: Forces all required taxons to be optional
        """
        self._context = context
        self._parse = parse
        self._skip_root_node = skip_root_node
        self._force_optional_taxon = force_optional_taxon

    def visitParse(self, ctx: TelParser.ParseContext):
        if self._context.depth == 0 and not self._skip_root_node:
            return TelRoot.new(parser_context_to_location(ctx), self._context, self.visit(ctx.expr()))
        else:
            return self.visit(ctx.expr())

    def visitBracketExpr(self, ctx: TelParser.BracketExprContext):
        return TelParentheses.new(parser_context_to_location(ctx), self._context, self.visit(ctx.expr()))

    def visitNullTestExpr(self, ctx: TelParser.NullTestExprContext) -> TelExpression:
        if ctx.NOT():
            return TelIsNotNull.new(parser_context_to_location(ctx), self._context, self.visit(ctx.expr()))
        else:
            return TelIsNull.new(parser_context_to_location(ctx), self._context, self.visit(ctx.expr()))

    def visitNotExpr(self, ctx: TelParser.NotExprContext) -> TelExpression:
        return TelNot.new(parser_context_to_location(ctx), self._context, self.visit(ctx.expr()))

    def visitLogicalExpr(self, ctx: TelParser.LogicalExprContext) -> TelExpression:
        left = self.visit(ctx.expr(0))
        right = self.visit(ctx.expr(1))
        op = self._TEL_OPERATOR_SQL_MAPPING.get(ctx.children[1].symbol.type, str(ctx.children[1]))
        return TelLogicalOperation.new(parser_context_to_location(ctx), self._context, op, left, right)

    def visitMultiplicationExpr(self, ctx: TelParser.MultiplicationExprContext) -> TelExpression:
        left = self.visit(ctx.expr(0))
        right = self.visit(ctx.expr(1))

        if ctx.MULT():
            return TelMultiplication.new(parser_context_to_location(ctx), self._context, '*', left, right)
        else:
            return TelDivision.new(parser_context_to_location(ctx), self._context, '/', left, right)

    def visitAdditiveExpr(self, ctx: TelParser.AdditiveExprContext) -> TelExpression:
        left = self.visit(ctx.expr(0))
        right = self.visit(ctx.expr(1))

        if ctx.PLUS():
            return TelAddition.new(parser_context_to_location(ctx), self._context, '+', left, right)
        else:
            return TelSubtraction.new(parser_context_to_location(ctx), self._context, '-', left, right)

    def visitNumberAtom(self, ctx: TelParser.NumberAtomContext) -> TelExpression:
        if ctx.INT():
            return TelInteger.new(parser_context_to_location(ctx), self._context, int(ctx.getText()))
        else:
            return TelFloat.new(parser_context_to_location(ctx), self._context, float(ctx.getText()))

    def visitFn(self, ctx: TelParser.FnContext) -> TelExpression:
        name = ctx.WORD().getText()
        if name not in TEL_FUNCTIONS:
            self._context.validation_context.with_error(
                f'Unknown function {name}', location=parser_context_to_location(ctx)
            )
            raise self._context.validation_context.exception
        else:
            ctor = TEL_FUNCTIONS[name]
            return ctor.new(parser_context_to_location(ctx), self._context, [self.visit(arg) for arg in ctx.expr()])

    def visitBooleanAtom(self, ctx: TelParser.BooleanAtomContext) -> TelExpression:
        return TelBoolean.new(parser_context_to_location(ctx), self._context, True if ctx.TRUE() else False)

    def visitTaxon_expr(self, ctx: TelParser.Taxon_exprContext):
        words = ctx.taxon().WORD()

        name = words[1 if ctx.taxon().TAXON_NAMESPACE_DELIMITER() else 0].getText()
        namespace = words[0].getText() if ctx.taxon().TAXON_NAMESPACE_DELIMITER() else None

        slug = cast(TaxonExpressionStr, f"{(namespace + '|') if namespace else ''}{name}")
        taxon = self._context.taxon_from_slug(slug)

        optional = True if ctx.OPTIONAL_TAXON_OPERATOR() else False

        calculation_expr = None
        if taxon and taxon.calculation:
            force_optional_taxon = self._force_optional_taxon or optional
            calculation_expr = self._parse(taxon.calculation, self._context.nested, False, force_optional_taxon)

        if optional or self._force_optional_taxon:
            return self._context.tel_dialect.optional_taxon_ctor.new(
                parser_context_to_location(ctx), self._context, name, namespace, slug, taxon, calculation_expr
            )
        else:
            return self._context.tel_dialect.required_taxon_ctor.new(
                parser_context_to_location(ctx), self._context, name, namespace, slug, taxon, calculation_expr
            )

    def visitStringConstantAtom(self, ctx: TelParser.StringConstantAtomContext) -> TelExpression:
        return self._context.tel_dialect.double_quoted_string_ctor.new(
            parser_context_to_location(ctx), self._context, ctx.getText()[1:-1]
        )

    def visitSingleQuotedAtom(self, ctx: TelParser.SingleQuotedAtomContext):
        return TelString.new(parser_context_to_location(ctx), self._context, ctx.getText()[1:-1])

    def visitAtomExpr(self, ctx: TelParser.AtomExprContext):
        return self.visit(ctx.atom())


class TelErrorListener(ErrorListener):
    def __init__(self, input: str):
        self._input = input

    def syntaxError(
        self, recognizer: Recognizer, offending_symbol: Token, line: int, column: int, msg: str, e: RecognitionException
    ):
        raise TelExpressionException.create_with_message(
            f'Unexpected symbol "{offending_symbol.text}"' if offending_symbol else msg,
            column + 1,
            line,
            self._input.strip(),
        )


def parser_context_to_location(ctx: ParserRuleContext) -> ParserLocation:
    position = ctx.start.start + 1
    line = ctx.start.line
    text = str(ctx.start.source[1])

    return ParserLocation(position, line, text)
