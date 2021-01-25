from collections import defaultdict
from typing import Dict, List, Set

from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.core.federated.model.models import (
    FdqModel,
    FdqModelAttribute,
    FdqModelJoin,
    FdqModelJoinRelationship,
)
from panoramic.cli.husky.core.federated.utils import (
    prefix_with_virtual_data_source,
    remove_virtual_data_source_prefix,
)
from panoramic.cli.husky.core.model.enums import HuskyModelType
from panoramic.cli.husky.core.model.models import HuskyModel, ModelAttribute, ModelJoin


class FdqModelAttributeMapper:
    """
    Mapper helper class for FdqModelAttribute
    """

    @classmethod
    def from_internal(
        cls,
        transformation: str,
        model_attributes: List[ModelAttribute],
        virtual_data_source: str,
    ) -> FdqModelAttribute:
        """
        Initialize model attribute for API model from Husky model attribute
        and cutoff virtual data source from taxon slug, if needed
        """
        # type is same across all attributes
        data_type = model_attributes[0].column_sql_type

        return FdqModelAttribute.construct(
            field_map=[remove_virtual_data_source_prefix(virtual_data_source, attr.taxon) for attr in model_attributes],
            # for backward compatibility, we reuse column_name for a bit
            data_reference=transformation,
            data_type=EnumHelper.from_value_safe(DbDataType, data_type),
        )

    @classmethod
    def to_internal(
        cls, attr: FdqModelAttribute, virtual_data_source: str, identifiers: Set[str]
    ) -> List[ModelAttribute]:
        """
        Initialize model attribute for Husky model from API model attribute
        and add virtual data source taxon slug, if needed
        """
        attrs = []
        for taxon_slug in attr.field_map:
            attrs.append(
                ModelAttribute(
                    {
                        'tel_transformation': attr.data_reference,
                        'taxon': prefix_with_virtual_data_source(virtual_data_source, taxon_slug).lower(),
                        'identifier': taxon_slug in identifiers,
                        'column_sql_type': None if attr.data_type is None else attr.data_type.value,
                    }
                )
            )

        return attrs


class FdqModelJoinMapper:
    """
    Mapper helper class for FdqModelJoin
    """

    @classmethod
    def from_internal(cls, model_join: ModelJoin, virtual_data_source: str) -> 'FdqModelJoin':
        """
        Initialize model join for API model from Husky model join
        and cutoff virtual data source, if necessary
        """
        return FdqModelJoin.construct(
            join_type=model_join.join_type,
            to_model=remove_virtual_data_source_prefix(virtual_data_source, model_join.to_model),
            relationship=FdqModelJoinRelationship(model_join.relationship.value),
            taxons=[
                remove_virtual_data_source_prefix(virtual_data_source, taxon) for taxon in (model_join.taxons or [])
            ],
        )

    @classmethod
    def to_internal(cls, join: FdqModelJoin, virtual_data_source: str) -> ModelJoin:
        """
        Initialize model attribute for Husky model from API model attribute
        and add virtual data source taxon slug, if needed
        """
        return ModelJoin(
            {
                'join_type': join.join_type.value,
                # TODO ? make sure that "to_model" exists
                'to_model': prefix_with_virtual_data_source(virtual_data_source, join.to_model).lower(),
                'relationship': join.relationship.value,
                'taxons': [
                    prefix_with_virtual_data_source(virtual_data_source, taxon).lower() for taxon in join.taxons
                ],
            }
        )


class FdqModelMapper:
    """
    Mapper helper class for FdqModel
    """

    @classmethod
    def from_internal(cls, husky_model: HuskyModel) -> FdqModel:
        """
        Creates ApiModel from HuskyModel

        :param husky_model: Original HuskyModel

        :return: Correct FdqModel
        """
        virtual_data_source = husky_model.data_sources[0]

        identifiers = [
            remove_virtual_data_source_prefix(virtual_data_source, taxon_slug)
            for (taxon_slug, attr) in husky_model.attributes.items()
            if attr.identifier
        ]

        attrs_by_key: Dict[str, List[ModelAttribute]] = defaultdict(list)
        for attr in husky_model.attributes.values():
            attrs_by_key[attr.tel_transformation].append(attr)

        inst = FdqModel.construct(
            model_name=remove_virtual_data_source_prefix(virtual_data_source, husky_model.name),
            data_source='.'.join(husky_model.fully_qualified_name_parts or []),
            attributes=[
                FdqModelAttributeMapper.from_internal(transformation, attrs, virtual_data_source)
                for (transformation, attrs) in attrs_by_key.items()
            ],
            joins=[FdqModelJoinMapper.from_internal(join, virtual_data_source) for join in husky_model.joins],
            identifiers=identifiers,
            visibility=husky_model.visibility,
        )
        return inst

    @classmethod
    def to_internal(cls, model: FdqModel, virtual_data_source: str, company_id: str) -> HuskyModel:
        """
        Creates HuskyModel from FdqModel

        :return: Correct HuskyModel
        """
        identifiers = set(model.identifiers)

        data = {
            'name': prefix_with_virtual_data_source(virtual_data_source, model.model_name).lower(),
            'data_sources': [virtual_data_source],
            'model_type': HuskyModelType.METRIC,
            'fully_qualified_name_parts': model.data_source.split('.'),
            'visibility': model.visibility,
            'company_id': company_id,
            'attributes': {
                attr.taxon: attr
                for api_attr in model.attributes
                for attr in FdqModelAttributeMapper.to_internal(api_attr, virtual_data_source, identifiers)
            },
            'joins': [FdqModelJoinMapper.to_internal(join, virtual_data_source) for join in model.joins],
        }
        inst = HuskyModel(data)
        return inst
