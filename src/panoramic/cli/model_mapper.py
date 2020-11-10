import itertools
import operator
from typing import Dict, Iterable

from panoramic.cli.model.client import Model, ModelField, ModelJoin
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
        fields=join.fields, join_type=join.join_type, relationship=join.relationship, to_model=join.to_model
    )


def map_model_join_from_local(join: PanoModelJoin) -> ModelJoin:
    """Convert local join to remote join."""
    return ModelJoin(
        fields=join.fields, join_type=join.join_type, relationship=join.relationship, to_model=join.to_model
    )


def map_model_field_from_remote(field: ModelField) -> PanoModelField:
    """Convert remote attributes to local field."""
    return PanoModelField(field_map=field.field_map, data_reference=field.data_reference)


def map_attributes_from_local(field: PanoModelField) -> ModelField:
    """Convert local field to remote attributes."""
    return ModelField(
        data_reference=field.data_reference,
        field_map=field.field_map,
    )


def map_model_from_remote(model: Model) -> PanoModel:
    """Convert remote model to local model."""
    return PanoModel(
        model_name=model.model_name,
        data_source=model.data_source,
        fields=[map_model_field_from_remote(f) for f in model.fields],
        joins=[map_model_join_from_remote(j) for j in model.joins],
        identifiers=model.identifiers,
        virtual_data_source=model.virtual_data_source,
    )


def map_model_from_local(model: PanoModel) -> Model:
    """Convert local model to remote model."""
    return Model(
        model_name=model.model_name,
        data_source=model.data_source,
        fields=[map_attributes_from_local(f) for f in model.fields],
        joins=[map_model_join_from_local(j) for j in model.joins],
        identifiers=model.identifiers,
        visibility='available',  # default to available visibility
        virtual_data_source=model.virtual_data_source,
    )


def map_columns_to_model(raw_columns: Iterable[Dict]) -> Iterable[PanoModel]:
    """
    Load result of metadata table columns scan into Model
    """
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('data_source', 'model_name'))

    for (data_source, model_name), columns in columns_grouped:
        yield PanoModel(
            model_name=model_name,
            data_source=data_source,
            fields=[
                PanoModelField(
                    data_reference=col['data_reference'],
                    field_map=col['field_map'],
                )
                for col in columns
            ],
            joins=[],
            identifiers=[],
        )
