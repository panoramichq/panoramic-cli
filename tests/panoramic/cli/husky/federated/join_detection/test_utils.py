from panoramic.cli.husky.core.federated.model.models import FdqModel
from panoramic.cli.husky.core.model.enums import ModelVisibility


def create_temp_internal_from_api_model(**kwargs) -> FdqModel:
    return FdqModel(
        model_name=kwargs.get('name', 'test_model'),
        data_source=kwargs.get('data_source', 'source.db.schema.table'),
        fields=kwargs.get('fields', []),
        identifiers=kwargs.get('identifiers', []),
        visibility=ModelVisibility.hidden,
    )
