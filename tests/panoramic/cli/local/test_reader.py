from pathlib import Path
from unittest.mock import Mock

from panoramic.cli.local.reader import FilePackage, FileReader
from panoramic.cli.paths import FileExtension, PresetFileName, SystemDirectory


def test_reader_get_packages(tmp_path: Path):
    # scanned directory
    scanned_dir = tmp_path / SystemDirectory.SCANNED.value
    scanned_dir.mkdir()
    (scanned_dir / PresetFileName.DATASET_YAML.value).touch()

    # dataset with one model
    ds1_dir = tmp_path / 'dataset1'
    ds1_dir.mkdir()

    ds1_file = ds1_dir / PresetFileName.DATASET_YAML.value
    ds1_file.touch()

    model_file = ds1_dir / f'test_model{FileExtension.MODEL_YAML.value}'
    model_file.touch()

    # empty dataset
    ds2_dir = tmp_path / 'dataset2'
    ds2_dir.mkdir()

    packages = FileReader(cwd=tmp_path).get_packages()

    assert list(packages) == [FilePackage(name='dataset1', data_source_file=ds1_file, model_files=[model_file])]


def test_file_package_read_data_source(tmp_path):
    ds_file = tmp_path / PresetFileName.DATASET_YAML.value
    with ds_file.open('w') as f:
        f.write('dataset_slug: test-dataset')

    package = FilePackage(name='dataset1', data_source_file=ds_file, model_files=[])

    assert package.read_data_source() == {'dataset_slug': 'test-dataset'}


def test_file_package_read_models(tmp_path):
    model_file = tmp_path / f'test_model{FileExtension.MODEL_YAML.value}'
    with model_file.open('w') as f:
        f.write('model_name: test-model')

    package = FilePackage(name='dataset1', data_source_file=Mock(), model_files=[model_file])

    assert list(package.read_models()) == [({'model_name': 'test-model'}, model_file)]
