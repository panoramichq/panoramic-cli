from typing import Iterable, Optional

from panoramic.cli.husky.core.tel.result import UsedTaxonSlugsContainer
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    data_source_from_slug,
)
from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser
from panoramic.cli.tel_grammar.TelVisitor import TelVisitor as AntlrTelVisitor


class TelUsedTaxonsVisitor(AntlrTelVisitor):
    """
    Visitor that will return taxons used in the Tel Expression, filtered by provided by set of data sources.
    """

    # TODO this can be removed and core.evaluator.TelVisitor can be used instead

    def __init__(self, data_sources: Optional[Iterable[str]]):
        self._used_taxons = UsedTaxonSlugsContainer()
        self._data_sources = data_sources

    def visitParse(self, ctx: AntlrTelParser.ParseContext) -> UsedTaxonSlugsContainer:
        super().visitParse(ctx)
        return self._used_taxons

    def visitTaxon_expr(self, ctx: AntlrTelParser.Taxon_exprContext):
        optional = True if ctx.OPTIONAL_TAXON_OPERATOR() else False
        slug = self.visit(ctx.taxon())

        data_source = data_source_from_slug(slug)

        if self._data_sources is not None:
            if not data_source or data_source in self._data_sources:
                self._used_taxons.add_slug(slug, optional)
        else:
            self._used_taxons.add_slug(slug, optional)

    def visitTaxon(self, ctx: AntlrTelParser.TaxonContext) -> str:
        return ctx.getText()
