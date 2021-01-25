import pytest

from panoramic.cli.husky.core.model.enums import ModelVisibility
from panoramic.cli.husky.service.constants import TaxonSlugs
from panoramic.cli.husky.service.model_retriever.model_augments import ModelAugments
from tests.panoramic.cli.husky.test.mocks.husky_model import generate_husky_mock_model


@pytest.mark.parametrize(
    'inp_model,expected_model',
    [
        (
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                project_id='project_2',
                attributes={'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True}},
            ),
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                attributes={
                    'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True},
                    TaxonSlugs.COMPANY_ID: {
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'identifier': False,
                        'tel_transformation': "'company_id'",
                    },
                    TaxonSlugs.PROJECT_ID: {
                        'taxon': TaxonSlugs.PROJECT_ID,
                        'identifier': False,
                        'tel_transformation': "'project_2'",
                    },
                },
                project_id='project_2',
            ),
        ),
        (
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                company_id='cid_1',
                attributes={'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True}},
            ),
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                company_id='cid_1',
                attributes={
                    'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True},
                    TaxonSlugs.COMPANY_ID: {
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'identifier': False,
                        'tel_transformation': "'cid_1'",
                    },
                },
            ),
        ),
        (
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                project_id='project_2',
                company_id='cid_1',
                attributes={'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True}},
            ),
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                project_id='project_2',
                company_id='cid_1',
                attributes={
                    'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True},
                    TaxonSlugs.PROJECT_ID: {
                        'taxon': TaxonSlugs.PROJECT_ID,
                        'identifier': False,
                        'tel_transformation': "'project_2'",
                    },
                    TaxonSlugs.COMPANY_ID: {
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'identifier': False,
                        'tel_transformation': "'cid_1'",
                    },
                },
            ),
        ),
        (
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                project_id='project_2',
                company_id='cid_1',
                attributes={
                    'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True},
                    TaxonSlugs.PROJECT_ID: {
                        'tel_transformation': '"column_a"',
                        'taxon': TaxonSlugs.PROJECT_ID,
                        'identifier': False,
                    },
                    TaxonSlugs.COMPANY_ID: {
                        'tel_transformation': '"column_b"',
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'identifier': False,
                    },
                },
            ),
            generate_husky_mock_model(
                visibility=ModelVisibility.available,
                project_id='project_2',
                company_id='cid_1',
                attributes={
                    'ad_id': {'tel_transformation': '"ad_id"', 'taxon': 'ad_id', 'identifier': True},
                    TaxonSlugs.PROJECT_ID: {
                        'tel_transformation': '"column_a"',
                        'taxon': TaxonSlugs.PROJECT_ID,
                        'identifier': False,
                    },
                    TaxonSlugs.COMPANY_ID: {
                        'tel_transformation': '"column_b"',
                        'taxon': TaxonSlugs.COMPANY_ID,
                        'identifier': False,
                    },
                },
            ),
        ),
    ],
)
def test_model_add_model_info_attributes(inp_model, expected_model):
    ModelAugments._model_add_model_info_attributes(inp_model)
    assert inp_model.to_primitive() == expected_model.to_primitive()
