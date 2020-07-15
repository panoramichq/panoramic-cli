from panoramic.cli.local.reader import FileReader
from panoramic.cli.pano_model import PanoModel, PanoVirtualDataSource
from panoramic.cli.state import VirtualState


def get_state() -> VirtualState:
    """Build a representation of what VDS and models are on local filesystem."""
    packages = FileReader().get_packages()
    data_sources = []
    models = []
    for package in packages:
        data_source = package.data_source
        data_source['package'] = package.name
        pvds = PanoVirtualDataSource.from_dict(data_source)
        data_sources.append(pvds)
        for model in package.models:
            model['package'] = package.name
            model['virtual_data_source'] = pvds.dataset_slug
            models.append(PanoModel.from_dict(model))

    return VirtualState(data_sources=data_sources, models=models)
