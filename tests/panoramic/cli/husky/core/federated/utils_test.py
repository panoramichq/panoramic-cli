import pytest

from panoramic.cli.husky.core.federated.model.exceptions import WrongVirtualDataSource
from panoramic.cli.husky.core.federated.utils import (
    prefix_with_virtual_data_source,
    remove_virtual_data_source_prefix,
)


def test_prefix_with_virtual_data_source():
    slug = prefix_with_virtual_data_source('virtual', 'virtual_taxon_slug')
    assert slug == 'virtual|virtual_taxon_slug'


@pytest.mark.parametrize('vs,slug', [('vs', 'as|test'), ('vs', 'vs|test')])
def test_prefix_with_virtual_data_source_fails(vs, slug):
    with pytest.raises(WrongVirtualDataSource):
        prefix_with_virtual_data_source(vs, slug)


def test_remove_virtual_data_source_prefix():
    slug = remove_virtual_data_source_prefix('virtual', 'virtual|virtual_taxon_slug')
    assert slug == 'virtual_taxon_slug'


@pytest.mark.parametrize('vs,slug', [('vs', 'test'), ('vs', 'vs2|test')])
def test_remove_virtual_data_source_prefix_fails(vs, slug):
    with pytest.raises(WrongVirtualDataSource):
        remove_virtual_data_source_prefix(vs, slug)
