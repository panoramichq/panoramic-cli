from panoramic.cli.config.companies import get_company_id
from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.core.federated.model.mappers import FdqModelMapper
from panoramic.cli.husky.core.federated.model.models import FdqModel
from panoramic.cli.husky.core.model.enums import ModelVisibility
from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.constants import NAMESPACE_DELIMITER
from panoramic.cli.husky.core.taxonomy.enums import DisplayState, ValidationType
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.virtual_data_sources.models import VirtualDataSource
from panoramic.cli.husky.core.virtual_state.models import VirtualState
from panoramic.cli.pano_model import PanoField, PanoModel, PanoVirtualDataSource
from panoramic.cli.state import VirtualState as PanoVirtualState


class VirtualDataSourceMapper:
    """Mappers for internal virtual data source representation"""

    @staticmethod
    def to_husky(original: PanoVirtualDataSource) -> VirtualDataSource:
        """Maps external virtual data source to its internal representation"""

        return VirtualDataSource(slug=original.dataset_slug, display_name=original.display_name)


class FieldMapper:
    """Mappers for internal taxon representations"""

    @staticmethod
    def to_husky(origin: PanoField) -> Taxon:
        """Maps external field definitions to internal taxon representation"""

        slug = origin.slug if origin.data_source is None else f'{origin.data_source}{NAMESPACE_DELIMITER}{origin.slug}'
        aggregation = None
        if origin.aggregation:
            aggregation = AggregationDefinition.parse_obj(origin.aggregation.to_dict())

        validation = EnumHelper.from_value(ValidationType, origin.data_type)
        assert validation

        return Taxon(
            slug=slug,
            taxon_group=origin.group,
            display_name=origin.display_name,
            taxon_type=origin.field_type,
            validation_type=validation,
            taxon_description=origin.description,
            data_source=origin.data_source,
            calculation=origin.calculation,
            aggregation=aggregation,
            display_state=DisplayState.visible,
            company_id=get_company_id(),
        )


class ModelMapper:
    """Mappers for internal husky model"""

    @staticmethod
    def to_husky(origin: PanoModel) -> HuskyModel:
        """Maps external panoramic model to internal husky model"""
        assert origin.virtual_data_source

        fdq_model = FdqModel.parse_obj({**origin.to_dict(), 'visibility': ModelVisibility.available})
        husky_model = FdqModelMapper.to_internal(fdq_model, origin.virtual_data_source, get_company_id())

        return husky_model


class VirtualStateMapper:
    """Mappers for internal virtual state"""

    @staticmethod
    def to_husky(original: PanoVirtualState) -> VirtualState:
        """Maps external virtual state to internal virtual state"""

        return VirtualState(
            vds=[VirtualDataSourceMapper.to_husky(orig) for orig in original.data_sources],
            models=[ModelMapper.to_husky(orig) for orig in original.models],
            taxons=[FieldMapper.to_husky(orig) for orig in original.fields],
        )
