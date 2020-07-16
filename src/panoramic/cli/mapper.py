import itertools
from collections import defaultdict
from typing import Iterable, List, Set

from panoramic.cli.model.client import Model, ModelAttribute, ModelJoin
from panoramic.cli.pano_model import (
    PanoModel,
    PanoModelField,
    PanoModelJoin,
    PanoVirtualDataSource,
)
from panoramic.cli.virtual_data_source.client import VirtualDataSource


def map_data_source_from_remote(vds: VirtualDataSource) -> PanoVirtualDataSource:
    """Convert remote data source to local data source."""
    return PanoVirtualDataSource(dataset_slug=vds.slug, display_name=vds.display_name)


def map_data_source_from_local(vds: PanoVirtualDataSource) -> VirtualDataSource:
    """Convert local data source to remote data source."""
    return VirtualDataSource(slug=vds.dataset_slug, display_name=vds.display_name)


def map_model_join_from_remote(join: ModelJoin) -> PanoModelJoin:
    """Convert remote join to local join."""
    return PanoModelJoin(
        fields=join.taxons, join_type=join.join_type, relationship=join.relationship, to_model=join.to_model
    )


def map_model_join_from_local(join: PanoModelJoin) -> ModelJoin:
    """Convert local join to remote join."""
    return ModelJoin(
        taxons=join.fields, join_type=join.join_type, relationship=join.relationship, to_model=join.to_model
    )


def map_field_from_remote(transformation: str, attributes: List[ModelAttribute]) -> PanoModelField:
    """Convert remote attributes to local field."""
    # type is same across all attributes
    data_type = attributes[0].column_data_type
    assert data_type is not None
    return PanoModelField(field_map=[a.taxon for a in attributes], transformation=transformation, data_type=data_type,)


def map_attributes_from_local(field: PanoModelField, identifiers: Set[str]) -> Iterable[ModelAttribute]:
    """Convert local field to remote attributes."""
    for field_name in field.field_map:
        yield ModelAttribute(
            column_data_type=field.data_type,
            taxon=field_name,
            identifier=field_name in identifiers,
            transformation=field.transformation,
        )


def map_model_from_remote(model: Model) -> PanoModel:
    """Convert remote model to local model."""
    attrs_by_key = defaultdict(list)
    for attr in model.attributes:
        attrs_by_key[attr.transformation].append(attr)

    return PanoModel(
        model_name=model.name,
        data_source=model.fully_qualified_object_name,
        fields=[map_field_from_remote(transformation, attrs) for (transformation, attrs) in attrs_by_key.items()],
        joins=[map_model_join_from_remote(j) for j in model.joins],
        identifiers=[a.taxon for a in model.attributes if a.identifier],
        virtual_data_source=model.virtual_data_source,
    )


def map_model_from_local(model: PanoModel) -> Model:
    """Convert local model to remote model."""
    return Model(
        name=model.model_name,
        fully_qualified_object_name=model.data_source,
        attributes=list(
            itertools.chain.from_iterable(map_attributes_from_local(f, set(model.identifiers)) for f in model.fields)
        ),
        joins=[map_model_join_from_local(j) for j in model.joins],
        visibility='available',  # default to available visibility
        virtual_data_source=model.virtual_data_source,
    )
