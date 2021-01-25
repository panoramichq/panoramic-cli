from typing import Iterable, Optional, cast

from antlr4 import CommonTokenStream, InputStream
from typing_extensions import Protocol

from panoramic.cli.husky.core.tel.result import (
    UsedTaxonsContainer,
    UsedTaxonSlugsContainer,
)
from panoramic.cli.husky.core.tel.visitors.tel_used_taxons_visitor import (
    TelUsedTaxonsVisitor,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
)
from panoramic.cli.tel_grammar.TelLexer import TelLexer as AntlrTelLexer
from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser


class TaxonMapGetter(Protocol):
    def __call__(self, slugs: Iterable[str], must_load_all_taxons: Optional[bool] = True) -> TaxonMap:
        # https://stackoverflow.com/a/57840786
        raise NotImplementedError('This is only type, not a real callable.')


"""
Type of a fn to be used for fetching taxons.
"""


class Tel:
    @classmethod
    def get_used_taxon_slugs_shallow(
        cls, expr: str, data_sources: Optional[Iterable[str]] = None
    ) -> UsedTaxonSlugsContainer:
        """
        Shallow - only resolves the first level of expr, does not recursively resolve other taxon references.
        """
        instr = InputStream(expr)
        lexer = AntlrTelLexer(instr)
        stream = CommonTokenStream(lexer)
        parser = AntlrTelParser(stream)
        tree = parser.parse()
        visitor = TelUsedTaxonsVisitor(data_sources)
        return visitor.visit(tree)

    @classmethod
    def _get_aggregation_taxons(
        cls, taxons_map: TaxonMap, taxon_map_getter: TaxonMapGetter, must_load_all_taxons: bool
    ) -> TaxonMap:
        """
        Gets taxons used in aggregation definitions of the taxons in the map
        """
        agg_slugs = set()
        for taxon in taxons_map.values():
            if taxon.tel_metadata and taxon.tel_metadata.aggregation_definition:
                agg_slugs |= taxon.tel_metadata.aggregation_definition.used_taxon_slugs()

        if len(agg_slugs) == 0:
            return {}
        else:
            return taxon_map_getter(agg_slugs, must_load_all_taxons=must_load_all_taxons)

    @classmethod
    def get_all_used_taxons_map(
        cls,
        taxon_slugs: Iterable[str],
        data_sources: Optional[Iterable[str]],
        taxon_map_getter: TaxonMapGetter,
        include_aggregation_definition_taxons: bool = False,
    ) -> UsedTaxonsContainer:
        """
        Fetches all taxons requested and all the taxons used, recursively.
        Keeps tracks of required and optional taxons.
        Required taxons used as part of optional taxons are considered to be optional. Not ideal, but needed for simplicity for now.
        """
        used_taxons = UsedTaxonsContainer()
        used_slugs = UsedTaxonSlugsContainer()

        slugs_to_get = UsedTaxonSlugsContainer()
        # Init slugs to get
        slugs_to_get.required_slugs.update(cast(Iterable[TaxonExpressionStr], taxon_slugs))
        while len(slugs_to_get.required_slugs) or len(slugs_to_get.optional_slugs):
            loaded_taxons = UsedTaxonsContainer()
            if slugs_to_get.required_slugs:
                taxons_map = taxon_map_getter(slugs_to_get.required_slugs, must_load_all_taxons=True)
                loaded_taxons.required_taxons.update(taxons_map)

                # now, fetch taxons for aggregation definitions
                if include_aggregation_definition_taxons:
                    agg_taxons = cls._get_aggregation_taxons(taxons_map, taxon_map_getter, True)
                    loaded_taxons.required_taxons.update(agg_taxons)

            if slugs_to_get.optional_slugs:
                taxons_map = taxon_map_getter(slugs_to_get.optional_slugs, must_load_all_taxons=False)
                loaded_taxons.optional_taxons.update(taxons_map)

                # now, fetch taxons for aggregation definitions
                if include_aggregation_definition_taxons:
                    agg_taxons = cls._get_aggregation_taxons(taxons_map, taxon_map_getter, False)
                    loaded_taxons.optional_taxons.update(agg_taxons)

            used_taxons.update_from(loaded_taxons)
            slugs_to_get = UsedTaxonSlugsContainer()
            for taxon in loaded_taxons.required_taxons.values():
                if taxon.calculation:
                    _used_slugs_container = Tel.get_used_taxon_slugs_shallow(taxon.calculation, data_sources)
                    slugs_to_get.required_slugs.update(_used_slugs_container.required_slugs - used_slugs.required_slugs)
                    slugs_to_get.optional_slugs.update(_used_slugs_container.optional_slugs - used_slugs.optional_slugs)
            for taxon in loaded_taxons.optional_taxons.values():
                if taxon.calculation:
                    _used_slugs_container = Tel.get_used_taxon_slugs_shallow(taxon.calculation, data_sources)
                    # Required taxons from optional taxon are treated as optional
                    slugs_to_get.optional_slugs.update(_used_slugs_container.all_slugs - used_slugs.all_slugs)
            used_slugs.update_from(slugs_to_get)

        return used_taxons
