# Generated from grammar/Tel.g4 by ANTLR 4.8
from antlr4 import *

if __name__ is not None and "." in __name__:
    from .TelParser import TelParser
else:
    from TelParser import TelParser

# This class defines a complete generic visitor for a parse tree produced by TelParser.


class TelVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by TelParser#fn.
    def visitFn(self, ctx: TelParser.FnContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#taxon.
    def visitTaxon(self, ctx: TelParser.TaxonContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#taxon_expr.
    def visitTaxon_expr(self, ctx: TelParser.Taxon_exprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#parse.
    def visitParse(self, ctx: TelParser.ParseContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#nullTestExpr.
    def visitNullTestExpr(self, ctx: TelParser.NullTestExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#notExpr.
    def visitNotExpr(self, ctx: TelParser.NotExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#logicalExpr.
    def visitLogicalExpr(self, ctx: TelParser.LogicalExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#multiplicationExpr.
    def visitMultiplicationExpr(self, ctx: TelParser.MultiplicationExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#atomExpr.
    def visitAtomExpr(self, ctx: TelParser.AtomExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#additiveExpr.
    def visitAdditiveExpr(self, ctx: TelParser.AdditiveExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#bracketExpr.
    def visitBracketExpr(self, ctx: TelParser.BracketExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#numberAtom.
    def visitNumberAtom(self, ctx: TelParser.NumberAtomContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#fnExpr.
    def visitFnExpr(self, ctx: TelParser.FnExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#booleanAtom.
    def visitBooleanAtom(self, ctx: TelParser.BooleanAtomContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#taxonSlugAtom.
    def visitTaxonSlugAtom(self, ctx: TelParser.TaxonSlugAtomContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#singleQuotedAtom.
    def visitSingleQuotedAtom(self, ctx: TelParser.SingleQuotedAtomContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by TelParser#stringConstantAtom.
    def visitStringConstantAtom(self, ctx: TelParser.StringConstantAtomContext):
        return self.visitChildren(ctx)


del TelParser
