import os
from pathlib import Path
from unittest.mock import Mock

from panoramic.cli.local.reader import FilePackage, FileReader
from panoramic.cli.paths import FileExtension, Paths, PresetFileName, SystemDirectory


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

    ds1_fields_dir = Paths.fields_dir(ds1_dir)
    ds1_fields_dir.mkdir()
    field_file = ds1_fields_dir / f'test_field{FileExtension.FIELD_YAML.value}'
    field_file.touch()

    # empty dataset
    ds2_dir = tmp_path / 'dataset2'
    ds2_dir.mkdir()

    packages = list(FileReader(cwd=tmp_path).get_packages())
    expected = [
        FilePackage(name='dataset1', data_source_file=ds1_file, model_files=[model_file], field_files=[field_file])
    ]
    assert packages == expected


def test_file_package_read_data_source(tmp_path):
    ds_file = tmp_path / PresetFileName.DATASET_YAML.value
    with ds_file.open('w') as f:
        f.write('dataset_slug: test-dataset')

    package = FilePackage(name='dataset1', data_source_file=ds_file, model_files=[], field_files=[])

    assert package.read_data_source() == {'dataset_slug': 'test-dataset'}


def test_file_package_read_models(tmp_path):
    model_file = tmp_path / f'test_model{FileExtension.MODEL_YAML.value}'
    with model_file.open('w') as f:
        f.write('model_name: test-model')

    package = FilePackage(name='dataset1', data_source_file=Mock(), model_files=[model_file], field_files=[])

    assert list(package.read_models()) == [({'model_name': 'test-model'}, model_file)]


def test_file_package_read_fields(tmp_path):
    field_file = Paths.fields_dir(tmp_path) / f'test_field{FileExtension.FIELD_YAML.value}'
    os.makedirs(os.path.dirname(field_file), exist_ok=True)

    with field_file.open('w') as f:
        f.write('slug: field_slug')

    package = FilePackage(name='dataset1', data_source_file=Mock(), model_files=[], field_files=[field_file])

    assert list(package.read_fields()) == [({'slug': 'field_slug'}, field_file)]
