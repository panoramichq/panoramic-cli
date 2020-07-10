from panoramic.cli.local.file_utils import (
    FileExtension,
    FilePackage,
    delete_file,
    get_target_abs_filepath,
    write_yaml,
)
from panoramic.cli.pano_model import PanoModel


class FileWriter:
    def __init__(self, package: FilePackage):
        self.package = package

    def write_model(self, model: PanoModel):
        path = get_target_abs_filepath(model.table_file_name, FileExtension.model_yaml, self.package)
        write_yaml(path, model.to_dict())

    def delete_model(self, model: PanoModel):
        path = get_target_abs_filepath(model.table_file_name, FileExtension.model_yaml, self.package)
        delete_file(path)
