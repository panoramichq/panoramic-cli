import datetime
from unittest.mock import patch

from panoramic.cli.husky.core.model.enums import ModelVisibility, TimeGranularity
from panoramic.cli.husky.service.model_retriever.component import ModelRetriever
from panoramic.cli.husky.service.types.api_scope_types import Scope
from tests.panoramic.cli.husky.test.mocks.husky_model import generate_husky_mock_model
from tests.panoramic.cli.husky.test.test_base import BaseTest

mock_response = [
    generate_husky_mock_model(
        name='facebook_model',
        schema_name='FACEBOOK_VIEWS.test',
        visibility=ModelVisibility.available,
        company_id='company_1',
        project_id='project_1',
    ),
    generate_husky_mock_model(
        name='adwords_model',
        schema_name='ADWORDS_VIEWS.test',
        visibility=ModelVisibility.available,
        company_id='company_1',
        project_id='project_2',
    ),
    generate_husky_mock_model(
        name='company_wide_model',
        schema_name='ADWORDS_VIEWS.another_test',
        visibility=ModelVisibility.available,
        company_id='company_1',
        project_id=None,
    ),
    generate_husky_mock_model(
        name='specific_adwords_model',
        schema_name='ADWORDS_VIEWS.another_test',
        visibility=ModelVisibility.available,
        company_id='company_1',
        project_id='project_2',
    ),
    generate_husky_mock_model(
        name='specific_snap_model',
        schema_name='SNAPCHAT_VIEWS.test',
        object_name='snap_hourly',
        visibility=ModelVisibility.available,
        company_id='company_2',
        project_id='project_2',
        time_granularity=TimeGranularity.hour,
    ),
    generate_husky_mock_model(
        name='appnexus_model',
        schema_name='APPNEXUS_VIEWS.test',
        visibility=ModelVisibility.available,
        company_id='company_2',
        project_id='project_2',
    ),
    generate_husky_mock_model(
        name='some_other_model',
        data_sources=['some-other-source'],
        visibility=ModelVisibility.available,
        company_id='company_2',
        project_id='project_2',
    ),
    generate_husky_mock_model(
        name='a-hidden-model',
        data_sources=['another-special-data-source'],
        visibility=ModelVisibility.hidden,
        company_id='company_2',
        project_id='project_2',
    ),
    # Simulating an "incorrect" payload (that shouldn't happen in the wild) that has only project_id set - since
    # there is no company id it should not be shown anywhere because the "company_id" never matches. This is to
    # ensure we don't show other company's model because of a mistake in the snowflake data.
    generate_husky_mock_model(
        name='an-experimental-model',
        data_sources=['another-special-data-source'],
        visibility=ModelVisibility.experimental,
        company_id='company_2',
        project_id='project_2',
    ),
    generate_husky_mock_model(
        name='a-cached-model',
        data_sources=['some-other-special-data-source'],
        visibility=ModelVisibility.available,
        company_id='company_2',
        project_id='project_2',
        created_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=90),
        expire_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=90),
    ),
    generate_husky_mock_model(
        name='an-expired-model',
        data_sources=['some-other-special-data-source'],
        visibility=ModelVisibility.available,
        company_id='company_2',
        project_id='project_2',
        created_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=90),
        expire_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=80),
    ),
]


@patch(
    'panoramic.cli.husky.service.model_retriever.component.ModelRetriever._load_all_models',
    return_value=mock_response,
)
class ModelRetrieverTest(BaseTest):
    def test_filters_on_scope(self, _retriever_mock):
        scope = Scope(dict(company_id='company_1', project_id='project_1'))

        models = ModelRetriever.load_models(set(), scope)
        assert len(models) == 2

        assert mock_response[0] in models
        assert mock_response[2] in models
        assert next(x for x in mock_response if x.name == 'company_wide_model')

    def test_filters_data_sources(self, _retriever_mock):
        scope = Scope(dict(company_id='company_2', project_id='project_2'))

        models = ModelRetriever.load_models({'some-other-source'}, scope)
        assert len(models) == 1
        assert models[0] == next(x for x in mock_response if x.name == 'some_other_model')

    def test_specific_model_name(self, _retriever_mock):
        scope = Scope(dict(company_id='company_2', project_id='project_2'))

        models = ModelRetriever.load_models(set(), scope, 'specific_snap_model')

        assert len(models) == 1
        assert models[0].name == 'specific_snap_model'

    def test_model_augment(self, _retriever_mock):
        scope = Scope(dict(company_id='company_2', project_id='project_2'))

        models = ModelRetriever.load_models(set(), scope, 'specific_snap_model')

        assert len(models) == 1

        model = models[0]

        assert model.name == 'specific_snap_model'
        assert model.get_attribute_by_taxon('data_source').taxon == 'data_source'
        assert model.get_attribute_by_taxon('date_hour').taxon == 'date_hour'

    def test_visibility(self, _retriever_mock):
        from panoramic.cli.husky.service.utils.exceptions import ModelNotFoundException

        with self.assertRaises(ModelNotFoundException):
            scope = Scope(dict(company_id='company_2', project_id='project_2'))
            ModelRetriever.load_models({'another-special-data-source'}, scope)

    def test_includes_generally_available_models(self, _retriever_mock):
        scope = Scope(dict(company_id='company_2', project_id='project_2'))
        models = ModelRetriever.load_models(set(), scope)

        model_names = {x.name for x in models}
        assert all([x.visibility is ModelVisibility.available for x in models])
        assert 'an-invalid-model-that-shouldnt-be-displayed' not in model_names
        assert len(models) == 5

    def test_includes_experimental_if_scope_asks_for_them(self, _retriever_mock):
        scope = Scope(
            dict(
                company_id='company_2',
                project_id='project_2',
                model_visibility=ModelVisibility.experimental,
            )
        )

        models = ModelRetriever.load_models(set(), scope)
        model_names = {model.name for model in models}
        assert len(models) == 6, 'All available and experimental models are visible'
        assert 'an-experimental-model' in model_names
