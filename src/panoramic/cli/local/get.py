from typing import Optional

from panoramic.cli.local.reader import FileReader
from panoramic.cli.pano_model import PanoField, PanoModel, PanoVirtualDataSource
from panoramic.cli.state import VirtualState


def get_state(target_dataset: Optional[str] = None) -> VirtualState:
    """
    Build a representation of what VDS and models are on local filesystem.
    """
    file_reader = FileReader()
    packages = file_reader.get_packages()
    data_sources = []
    models = []
    fields = []

    if target_dataset is None:
        for field, path in file_reader.get_global_package().read_fields():
            field['file_name'] = path.name
            fields.append(PanoField.from_dict(field))

    for package in packages:
        data_source = package.read_data_source()
        data_source['package'] = package.name
        pvds = PanoVirtualDataSource.from_dict(data_source)

        if target_dataset is not None and target_dataset != pvds.dataset_slug:
            continue

        data_sources.append(pvds)
        for model, path in package.read_models():
            model['package'] = package.name
            model['file_name'] = path.name
            model['virtual_data_source'] = pvds.dataset_slug
            models.append(PanoModel.from_dict(model))

        for field, path in package.read_fields():
            field['package'] = package.name
            field['file_name'] = path.name
            field['data_source'] = pvds.dataset_slug
            fields.append(PanoField.from_dict(field))

    return VirtualState(data_sources=data_sources, models=models, fields=fields)
