import pytest

from panoramic.cli.husky.core.federated.taxonomy.mapper import unmmap_slug


@pytest.mark.parametrize(
    ['taxon_slug', 'expected'], [('vds|test_taxon', 'vds|test_taxon'), ('company_slug__test_taxon', 'test_taxon')]
)
def test_unmap_slug(taxon_slug, expected):
    result = unmmap_slug(taxon_slug)
    assert result == expected
