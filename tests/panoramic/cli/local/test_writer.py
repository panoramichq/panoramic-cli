from unittest.mock import Mock, call, patch

from panoramic.cli.local.writer import FileWriter
from panoramic.cli.pano_model import PanoField
from panoramic.cli.paths import FileExtension, PresetFileName, SystemDirectory


@patch('panoramic.cli.local.writer.write_yaml')
def test_writer_write_data_source_package_none(mock_write_yaml, tmp_path):
    mock_ds = Mock(dataset_slug='test_dataset')

    FileWriter(cwd=tmp_path).write_data_source(mock_ds)

    assert mock_write_yaml.mock_calls == [
        call(tmp_path / 'test_dataset' / PresetFileName.DATASET_YAML.value, mock_ds.to_dict.return_value)
    ]


@patch('panoramic.cli.local.writer.write_yaml')
def test_writer_write_data_source_package_set(mock_write_yaml, tmp_path):
    mock_ds = Mock(dataset_slug='test_dataset')

    FileWriter(cwd=tmp_path).write_data_source(mock_ds, package='my_package')

    assert mock_write_yaml.mock_calls == [
        call(tmp_path / 'my_package' / PresetFileName.DATASET_YAML.value, mock_ds.to_dict.return_value)
    ]


@patch('panoramic.cli.local.writer.delete_file')
def test_writer_delete_data_source(mock_delete_file, tmp_path):
    mock_ds = Mock(package='test_dataset')

    FileWriter(cwd=tmp_path).delete_data_source(mock_ds)

    assert mock_delete_file.mock_calls == [call(tmp_path / 'test_dataset' / PresetFileName.DATASET_YAML.value)]


@patch('panoramic.cli.local.writer.write_yaml')
def test_writer_write_field(mock_write_yaml, tmp_path):
    mock_field_company_scoped = Mock(spec=PanoField, slug='slug', data_source=None)
    mock_field_vds_scoped = Mock(spec=PanoField, slug='slug', data_source='test_dataset')

    FileWriter(cwd=tmp_path).write_field(mock_field_company_scoped)
    FileWriter(cwd=tmp_path).write_field(mock_field_vds_scoped)

    assert mock_write_yaml.mock_calls == [
        call(
            tmp_path
            / SystemDirectory.FIELDS.value
            / f'{mock_field_company_scoped.slug}{FileExtension.FIELD_YAML.value}',
            mock_field_company_scoped.to_dict.return_value,
        ),
        call(
            tmp_path
            / 'test_dataset'
            / SystemDirectory.FIELDS.value
            / f'{mock_field_vds_scoped.slug}{FileExtension.FIELD_YAML.value}',
            mock_field_vds_scoped.to_dict.return_value,
        ),
    ]


@patch('panoramic.cli.local.writer.delete_file')
def test_writer_delete_field(mock_delete_file, tmp_path):
    file_name = f'slug{FileExtension.FIELD_YAML.value}'
    mock_field_company_scoped = Mock(spec=PanoField, slug='slug', data_source=None, file_name=file_name)
    mock_field_vds_scoped = Mock(spec=PanoField, slug='slug', data_source='test_dataset', file_name=file_name)

    FileWriter(cwd=tmp_path).delete_field(mock_field_company_scoped)
    FileWriter(cwd=tmp_path).delete_field(mock_field_vds_scoped)

    assert mock_delete_file.mock_calls == [
        call(tmp_path / SystemDirectory.FIELDS.value / file_name),
        call(tmp_path / 'test_dataset' / SystemDirectory.FIELDS.value / file_name),
    ]
