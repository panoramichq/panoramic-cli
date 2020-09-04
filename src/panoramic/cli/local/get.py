from typing import Optional

from panoramic.cli.local.reader import FileReader
from panoramic.cli.pano_model import PanoModel, PanoVirtualDataSource
from panoramic.cli.state import VirtualState


def get_state(target_dataset: Optional[str] = None) -> VirtualState:
    """Build a representation of what VDS and models are on local filesystem.
    """
    packages = FileReader().get_packages()
    data_sources = []
    models = []
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

    return VirtualState(data_sources=data_sources, models=models)
