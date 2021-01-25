from typing import Dict, Set
from unittest.mock import patch

import pytest

from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.service.blending.search import Search
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingSearchRequest,
)
from panoramic.cli.husky.service.types.types import TaxonsByDataSource
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    get_mocked_taxons_by_slug,
    mock_get_taxons,
    mock_get_taxons_map,
)


class TestInput:
    def __init__(self, slugs_by_ds, computed_slugs):
        self.slugs_by_ds = slugs_by_ds
        self.computed_slugs = computed_slugs


def taxons_by_ds_to_slugs(taxons_by_ds: TaxonsByDataSource) -> Dict[str, Set[str]]:
    return {ds: {taxon.slug for taxon in taxons} for ds, taxons in taxons_by_ds.items()}


@pytest.mark.parametrize(
    "slugs_by_ds,computed_slugs,expected_slugs_by_ds",
    [
        (dict(adwords={'adwords|spend'}), ['fb_tw_spend_all_optional'], dict(adwords={'adwords|spend'})),
        (
            dict(facebook_ads={'facebook_ads|spend'}, twitter={'twitter|spend'}),
            ['computed_const'],
            dict(facebook_ads={'computed_const'}, twitter={'computed_const'}),
        ),
        (
            dict(facebook_ads={'spend', 'impressions'}, twitter={'twitter|spend', 'twitter|impressions'}),
            ['cpm'],
            dict(facebook_ads={'cpm'}, twitter=set()),
        ),
        (
            dict(facebook_ads={'facebook_ads|spend'}, twitter={'twitter|spend'}),
            ['fb_tw_spend_all_required', 'fb_tw_spend_all_optional'],
            dict(
                facebook_ads={'fb_tw_spend_all_required', 'fb_tw_spend_all_optional'},
                twitter={'fb_tw_spend_all_required', 'fb_tw_spend_all_optional'},
            ),
        ),
        (
            dict(facebook_ads={'facebook_ads|spend'}, twitter={'twitter|impressions'}),
            ['fb_tw_spend_all_optional'],
            dict(facebook_ads={'fb_tw_spend_all_optional'}),
        ),
        (
            dict(facebook_ads={'facebook_ads|spend'}, twitter={'twitter|impressions'}),
            ['fb_tw_adwords_spend_all_required'],
            dict(),
        ),
        (
            dict(
                facebook_ads={'facebook_ads|spend', 'facebook_ads|impressions', 'age_bucket', 'gender'},
                twitter={'twitter|spend', 'twitter|impressions', 'age_bucket'},
            ),
            [
                'generic_spend',
                'generic_cpm',
                'generic_impressions',
                'fb_tw_spend_all_required',
            ],
            dict(
                facebook_ads={
                    'generic_spend',
                    'generic_cpm',
                    'generic_impressions',
                    'fb_tw_spend_all_required',
                },
                twitter={
                    'generic_spend',
                    'generic_cpm',
                    'generic_impressions',
                    'fb_tw_spend_all_required',
                },
            ),
        ),
    ],
)
@patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
def test_expand_raw_taxons(mock_get_taxons, slugs_by_ds, computed_slugs, expected_slugs_by_ds):
    computed_taxons = list(mock_get_taxons_map(None, computed_slugs).values())
    raw_taxons_by_ds = {ds: get_mocked_taxons_by_slug(slugs) for ds, slugs in slugs_by_ds.items()}

    result_taxons_by_ds = Search.expand_raw_taxons(raw_taxons_by_ds, computed_taxons)
    result_slugs_by_ds = taxons_by_ds_to_slugs(result_taxons_by_ds)
    for ds, slugs in slugs_by_ds.items():
        if ds not in expected_slugs_by_ds:
            expected_slugs_by_ds[ds] = set()
        expected_slugs_by_ds[ds].update(slugs)

    assert result_slugs_by_ds == expected_slugs_by_ds


def _mock_get_taxons_allow_only_some(*args, **kwargs):
    """
    Filter all the mocked taxons, so this test wont fail just when someone adds new taxons that are using some basic
    taxons, like spend etc.
    :param args:
    :param kwargs:
    :return:
    """
    result = mock_get_taxons(*args, **kwargs)
    allowed = {
        'computed_const',
        'facebook_ads|impressions',
        'facebook_ads|spend',
        'fb_tw_adwords_spend_all_optional',
        'fb_tw_spend_all_optional',
        'fb_tw_spend_all_required',
        'gender',
        'generic_cpm',
        'generic_impressions',
        'generic_spend',
        'generic_spend2',
        'twitter|impressions',
        'twitter|spend',
    }
    return [taxon for taxon in result if taxon.slug in allowed]


@patch('panoramic.cli.husky.service.blending.search.HuskySearch')
@patch.object(Taxonomy, '_get_filtered_taxons', side_effect=_mock_get_taxons_allow_only_some)
def test_search_available_taxons(mock_get_taxons, mock_HuskySearch):
    mock_HuskySearch.get_available_raw_taxons.side_effect = [
        {'facebook_ads|spend', 'facebook_ads|impressions', 'gender'},
        {'twitter|spend', 'twitter|impressions'},
    ]
    search_request = BlendingSearchRequest(
        {
            "data_subrequests": [
                {
                    "scope": {"project_id": "328", "company_id": "50"},
                    "properties": {"data_sources": ["facebook_ads"]},
                    "taxons": ["account_id", "age_bucket"],
                },
                {
                    "scope": {"project_id": "328", "company_id": "50"},
                    "properties": {"data_sources": ["twitter"]},
                    "taxons": ["account_id", "age_bucket"],
                },
            ]
        }
    )
    result = Search.search_available_taxons(search_request)
    result_slugs = taxons_by_ds_to_slugs(result)
    assert result_slugs == {
        'facebook_ads': {
            'computed_const',
            'facebook_ads|impressions',
            'facebook_ads|spend',
            'fb_tw_adwords_spend_all_optional',
            'fb_tw_spend_all_optional',
            'fb_tw_spend_all_required',
            'gender',
            'generic_cpm',
            'generic_impressions',
            'generic_spend',
            'generic_spend2',
        },
        'twitter': {
            'computed_const',
            'fb_tw_adwords_spend_all_optional',
            'fb_tw_spend_all_optional',
            'fb_tw_spend_all_required',
            'generic_cpm',
            'generic_impressions',
            'generic_spend',
            'generic_spend2',
            'twitter|impressions',
            'twitter|spend',
        },
    }
