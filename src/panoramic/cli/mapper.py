import itertools
from collections import defaultdict
from typing import Iterable, List

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


def map_fields_from_remote(attributes: List[ModelAttribute]) -> Iterable[PanoModelField]:
    """Convert remote model to local model."""

    # Anything with same transformation is the same field
    attrs_by_key = defaultdict(list)
    for attr in attributes:
        attrs_by_key[attr.transformation].append(attr)

    for transformation, attrs in attrs_by_key.items():
        yield PanoModelField(
            field_map=[a.taxon for a in attrs],
            transformation=transformation,
            # assuming type is the same across all attributes
            data_type=attrs[0].column_data_type,
        )


def map_attributes_from_local(field: PanoModelField, identifiers: List[str]) -> Iterable[ModelAttribute]:
    """Convert local model to remote model."""
    for field_name in field.field_map:
        yield ModelAttribute(
            column_data_type=field.data_type,
            column_name=field.transformation,
            taxon=field_name,
            identifier=field_name in identifiers,
            transformation=field.transformation,
        )


def map_model_from_remote(model: Model) -> PanoModel:
    """Convert remote model to local model."""
    return PanoModel(
        model_name=model.name,
        data_source=model.fully_qualified_object_name,
        fields=list(map_fields_from_remote(model.attributes)),
        joins=[map_model_join_from_remote(j) for j in model.joins],
        identifiers=[a.taxon for a in model.attributes if a.identifier],
    )


def map_model_from_local(model: PanoModel) -> Model:
    """Convert local model to remote model."""
    return Model(
        name=model.model_name,
        fully_qualified_object_name=model.data_source,
        attributes=list(
            itertools.chain.from_iterable(map_attributes_from_local(f, model.identifiers) for f in model.fields)
        ),
        joins=[map_model_join_from_local(j) for j in model.joins],
        visibility='available',  # default to available visibility
        virtual_data_source=model.virtual_data_source,
    )
