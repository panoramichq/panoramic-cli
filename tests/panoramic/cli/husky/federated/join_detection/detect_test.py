from panoramic.cli.husky.core.federated.model.models import (
    FdqModelAttribute,
    FdqModelJoin,
    FdqModelJoinRelationship,
)
from panoramic.cli.husky.core.model.enums import JoinType
from panoramic.cli.husky.federated.join_detection.detect import detect_joins
from tests.panoramic.cli.husky.federated.join_detection.test_utils import (
    create_temp_internal_from_api_model,
)


def test_does_not_find_joins_on_empty_models():
    assert detect_joins([]) == {}


def test_does_not_find_joins_on_one_model():
    assert detect_joins(models=[create_temp_internal_from_api_model()]) == {}


def test_does_not_find_joins_on_models_without_ids():
    models = [
        create_temp_internal_from_api_model(name='first_model'),
        create_temp_internal_from_api_model(name='another_model'),
        create_temp_internal_from_api_model(name='third_model'),
    ]

    assert detect_joins(models=models) == {}


def test_does_not_find_joins_with_no_matching_ids():
    some_id_attr = FdqModelAttribute(data_reference='"some_id"', field_map=['some_id'])
    another_id_attr = FdqModelAttribute(data_reference='"another_id"', field_map=['another_id'])
    spend_attr = FdqModelAttribute(data_reference='"spend"', field_map=['spend'])

    model_one = create_temp_internal_from_api_model(
        name='model_1', data_source='source.db.schema.table', fields=[some_id_attr, spend_attr], identifiers=['some_id']
    )

    model_two = create_temp_internal_from_api_model(
        name='model_2',
        data_source='source.db.schema.table',
        fields=[another_id_attr, spend_attr],
        identifiers=['another_id'],
    )

    assert detect_joins(models=[model_one, model_two]) == {}


def test_detects_one_to_one():
    adset_id_attr = FdqModelAttribute(data_reference='"adset_id"', field_map=['adset_id'])
    spend_attr = FdqModelAttribute(data_reference='"spend"', field_map=['spend'])
    impressions_attr = FdqModelAttribute(data_reference='"impressions"', field_map=['impressions'])

    adset_model = create_temp_internal_from_api_model(
        name='model_1',
        data_source='source.db.schema.table',
        fields=[adset_id_attr, spend_attr],
        identifiers=['adset_id'],
    )

    another_adset_scoped_model = create_temp_internal_from_api_model(
        name='model_2',
        data_source='source.db.schema.second_table',
        fields=[adset_id_attr, impressions_attr],
        identifiers=['adset_id'],
    )

    models = [adset_model, another_adset_scoped_model]

    result = detect_joins(models)

    assert result == {
        adset_model.model_name: [
            FdqModelJoin(
                to_model=another_adset_scoped_model.model_name,
                relationship=FdqModelJoinRelationship.one_to_one,
                fields=['adset_id'],
                join_type=JoinType.left,
            ).dict(by_alias=True)
        ]
    }


def test_detects_many_to_one():
    adset_id_attr = FdqModelAttribute(data_reference='"adset_id"', field_map=['adset_id'])
    ad_id_attr = FdqModelAttribute(data_reference='"ad_id"', field_map=['ad_id'])
    spend_attr = FdqModelAttribute(data_reference='"spend"', field_map=['spend'])
    impressions_attr = FdqModelAttribute(data_reference='"impressions"', field_map=['impressions'])

    adset_model = create_temp_internal_from_api_model(
        name='model_1',
        data_source='source.db.schema.table',
        fields=[adset_id_attr, spend_attr],
        identifiers=['adset_id'],
    )
    ad_model = create_temp_internal_from_api_model(
        name='model_2',
        data_source='source.db.schema.second_table',
        fields=[ad_id_attr, adset_id_attr, spend_attr, impressions_attr],
        identifiers=['adset_id', 'ad_id'],
    )
    models = [adset_model, ad_model]

    result = detect_joins(models)

    assert result == {
        ad_model.model_name: [
            FdqModelJoin(
                to_model=adset_model.model_name,
                relationship=FdqModelJoinRelationship.many_to_one,
                fields=['adset_id'],
                join_type=JoinType.left,
            ).dict(by_alias=True)
        ]
    }
