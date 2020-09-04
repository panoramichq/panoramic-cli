from unittest.mock import Mock, call, patch

from panoramic.cli.local.writer import FileWriter
from panoramic.cli.paths import PresetFileName


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
