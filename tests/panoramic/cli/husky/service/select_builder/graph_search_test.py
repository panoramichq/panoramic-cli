from panoramic.cli.husky.service.select_builder.graph_search import (
    sort_models_with_heuristic,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonSlugExpression
from tests.panoramic.cli.husky.test.mocks.husky_model import (
    get_mock_entity_model,
    get_mock_metric_model,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestGraphSearch(BaseTest):
    def test_company_scoped_models(self):
        entity_model = get_mock_entity_model()
        company_model = get_mock_metric_model(company_id='company-id')
        models = [entity_model, company_model]
        sorted_models = sort_models_with_heuristic(
            models, {TaxonSlugExpression('ad_id'), TaxonSlugExpression('impressions')}
        )

        self.assertEqual(2, len(sorted_models))
        self.assertEqual([company_model, entity_model], sorted_models)
