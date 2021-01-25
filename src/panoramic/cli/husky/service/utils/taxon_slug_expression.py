from typing import Dict, NewType, Optional

from panoramic.cli.husky.core.taxonomy.constants import NAMESPACE_DELIMITER
from panoramic.cli.husky.core.taxonomy.models import Taxon

TaxonExpressionStr = NewType('TaxonExpressionStr', str)


def data_source_from_slug(slug: str) -> Optional[str]:
    """
    Returns data source from given slug. Returns None if slug does not have data source.
    :param slug:
    :return:
    """
    if NAMESPACE_DELIMITER in slug:
        splitted = slug.split(NAMESPACE_DELIMITER)
        assert len(splitted) == 2, f'Unexpected slug structure {slug}'
        return splitted[0]
    else:
        return None


class BasicTaxonSlugExpression:
    """
    Helper class for taxons slugs. Only contains information derivable from the actual slug.
    Use it's descendants for graph_slug properties etc.
    Slug might start with = if it is TEL expr in request, but no need to bother with it here so far,
    handled by BlendingTaxonManager.
    """

    def __init__(self, slug: str):
        self._slug = slug
        self.data_source = data_source_from_slug(slug)

    @property
    def slug(self):
        """
        Actual Taxon slug pressented to the client
        No return type so it can be used as str or as TaxonExpressionsStr
        """
        return self._slug

    def __hash__(self) -> int:
        return hash(self.slug)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BasicTaxonSlugExpression):
            return NotImplemented
        return self.slug == other.slug


class TaxonSlugExpression(BasicTaxonSlugExpression):
    def __init__(self, slug: str):
        super().__init__(slug)

    @property
    def graph_slug(self) -> str:
        """
        Taxon slug to search for in the model graph
        """
        return self._slug

    def __hash__(self) -> int:
        return hash(self.slug)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaxonSlugExpression):
            return NotImplemented
        return self.slug == other.slug

    def __repr__(self) -> str:
        return f'{self.slug} (graph_slug={self.graph_slug})'


TaxonMap = Dict[TaxonExpressionStr, Taxon]
SlugExprTaxonMap = Dict[TaxonSlugExpression, Taxon]
