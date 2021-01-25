# Generated from grammar/Tel.g4 by ANTLR 4.8
from antlr4 import *

if __name__ is not None and "." in __name__:
    from .TelParser import TelParser
else:
    from TelParser import TelParser

# This class defines a complete listener for a parse tree produced by TelParser.
class TelListener(ParseTreeListener):

    # Enter a parse tree produced by TelParser#fn.
    def enterFn(self, ctx: TelParser.FnContext):
        pass

    # Exit a parse tree produced by TelParser#fn.
    def exitFn(self, ctx: TelParser.FnContext):
        pass

    # Enter a parse tree produced by TelParser#taxon.
    def enterTaxon(self, ctx: TelParser.TaxonContext):
        pass

    # Exit a parse tree produced by TelParser#taxon.
    def exitTaxon(self, ctx: TelParser.TaxonContext):
        pass

    # Enter a parse tree produced by TelParser#taxon_expr.
    def enterTaxon_expr(self, ctx: TelParser.Taxon_exprContext):
        pass

    # Exit a parse tree produced by TelParser#taxon_expr.
    def exitTaxon_expr(self, ctx: TelParser.Taxon_exprContext):
        pass

    # Enter a parse tree produced by TelParser#parse.
    def enterParse(self, ctx: TelParser.ParseContext):
        pass

    # Exit a parse tree produced by TelParser#parse.
    def exitParse(self, ctx: TelParser.ParseContext):
        pass

    # Enter a parse tree produced by TelParser#nullTestExpr.
    def enterNullTestExpr(self, ctx: TelParser.NullTestExprContext):
        pass

    # Exit a parse tree produced by TelParser#nullTestExpr.
    def exitNullTestExpr(self, ctx: TelParser.NullTestExprContext):
        pass

    # Enter a parse tree produced by TelParser#notExpr.
    def enterNotExpr(self, ctx: TelParser.NotExprContext):
        pass

    # Exit a parse tree produced by TelParser#notExpr.
    def exitNotExpr(self, ctx: TelParser.NotExprContext):
        pass

    # Enter a parse tree produced by TelParser#logicalExpr.
    def enterLogicalExpr(self, ctx: TelParser.LogicalExprContext):
        pass

    # Exit a parse tree produced by TelParser#logicalExpr.
    def exitLogicalExpr(self, ctx: TelParser.LogicalExprContext):
        pass

    # Enter a parse tree produced by TelParser#multiplicationExpr.
    def enterMultiplicationExpr(self, ctx: TelParser.MultiplicationExprContext):
        pass

    # Exit a parse tree produced by TelParser#multiplicationExpr.
    def exitMultiplicationExpr(self, ctx: TelParser.MultiplicationExprContext):
        pass

    # Enter a parse tree produced by TelParser#atomExpr.
    def enterAtomExpr(self, ctx: TelParser.AtomExprContext):
        pass

    # Exit a parse tree produced by TelParser#atomExpr.
    def exitAtomExpr(self, ctx: TelParser.AtomExprContext):
        pass

    # Enter a parse tree produced by TelParser#additiveExpr.
    def enterAdditiveExpr(self, ctx: TelParser.AdditiveExprContext):
        pass

    # Exit a parse tree produced by TelParser#additiveExpr.
    def exitAdditiveExpr(self, ctx: TelParser.AdditiveExprContext):
        pass

    # Enter a parse tree produced by TelParser#bracketExpr.
    def enterBracketExpr(self, ctx: TelParser.BracketExprContext):
        pass

    # Exit a parse tree produced by TelParser#bracketExpr.
    def exitBracketExpr(self, ctx: TelParser.BracketExprContext):
        pass

    # Enter a parse tree produced by TelParser#numberAtom.
    def enterNumberAtom(self, ctx: TelParser.NumberAtomContext):
        pass

    # Exit a parse tree produced by TelParser#numberAtom.
    def exitNumberAtom(self, ctx: TelParser.NumberAtomContext):
        pass

    # Enter a parse tree produced by TelParser#fnExpr.
    def enterFnExpr(self, ctx: TelParser.FnExprContext):
        pass

    # Exit a parse tree produced by TelParser#fnExpr.
    def exitFnExpr(self, ctx: TelParser.FnExprContext):
        pass

    # Enter a parse tree produced by TelParser#booleanAtom.
    def enterBooleanAtom(self, ctx: TelParser.BooleanAtomContext):
        pass

    # Exit a parse tree produced by TelParser#booleanAtom.
    def exitBooleanAtom(self, ctx: TelParser.BooleanAtomContext):
        pass

    # Enter a parse tree produced by TelParser#taxonSlugAtom.
    def enterTaxonSlugAtom(self, ctx: TelParser.TaxonSlugAtomContext):
        pass

    # Exit a parse tree produced by TelParser#taxonSlugAtom.
    def exitTaxonSlugAtom(self, ctx: TelParser.TaxonSlugAtomContext):
        pass

    # Enter a parse tree produced by TelParser#singleQuotedAtom.
    def enterSingleQuotedAtom(self, ctx: TelParser.SingleQuotedAtomContext):
        pass

    # Exit a parse tree produced by TelParser#singleQuotedAtom.
    def exitSingleQuotedAtom(self, ctx: TelParser.SingleQuotedAtomContext):
        pass

    # Enter a parse tree produced by TelParser#stringConstantAtom.
    def enterStringConstantAtom(self, ctx: TelParser.StringConstantAtomContext):
        pass

    # Exit a parse tree produced by TelParser#stringConstantAtom.
    def exitStringConstantAtom(self, ctx: TelParser.StringConstantAtomContext):
        pass


del TelParser
