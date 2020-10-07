from typing import Any, Dict

import pytest
import yaml

from panoramic.cli.errors import FileMissingError, InvalidYamlFile, JsonSchemaError
from panoramic.cli.local.get import get_state
from panoramic.cli.paths import Paths, PresetFileName
from panoramic.cli.validate import (
    validate_config,
    validate_context,
    validate_local_state,
)

VALID_CONTEXT = {
    'api_version': 'v1',
    'company_slug': 'test_company',
}


INVALID_CONTEXTS = [
    # typo in version
    {'api_versio': 'v1', 'company_slug': 'test_company',},
    # wrong type in version
    {'api_version': 1, 'company_slug': 'test_company',},
    # typo in slug
    {'api_version': 'v1', 'company_slu': 'test_company'},
    # wrong type in slug
    {'api_version': 'v1', 'company_slug': 100},
]


@pytest.mark.parametrize('context', INVALID_CONTEXTS)
def test_validate_context_invalid(tmp_path, monkeypatch, context):
    monkeypatch.chdir(tmp_path)
    with Paths.context_file().open('w') as f:
        f.write(yaml.dump(context))

    with pytest.raises(JsonSchemaError):
        validate_context()


def test_validate_context_invalid_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with Paths.context_file().open('w') as f:
        f.write('not:\nyaml')

    with pytest.raises(InvalidYamlFile):
        validate_context()


def test_validate_context_missing_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileMissingError):
        validate_context()


def test_validate_context_valid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with Paths.context_file().open('w') as f:
        f.write(yaml.dump(VALID_CONTEXT))

    validate_context()


VALID_CONFIG = {
    'client_id': 'test-client-id',
    'client_secret': 'test-client-secret',
}


INVALID_CONFIGS = [
    # typo in client_id
    {'client_i': 'test_client_id', 'client_secret': 'test_client_secret'},
    # wrong type in client_id
    {'client_id': 100, 'client_secret': 'test_client_secret'},
    # typo in client_secret
    {'client_id': 'test_client_id', 'client_secre': 'test_client_secret'},
    # wrong type in client_secret
    {'client_id': 'test_client_id', 'client_secret': 200},
]


@pytest.mark.parametrize('config', INVALID_CONFIGS)
def test_validate_config_invalid_env_var_set(tmp_path, monkeypatch, config):
    monkeypatch.setenv('PANO_CLIENT_ID', 'test-client_id')
    monkeypatch.setenv('PANO_CLIENT_SECRET', 'test-client-secret')
    monkeypatch.setenv('HOME', str(tmp_path))

    Paths.config_dir().mkdir()
    with Paths.config_file().open('w') as f:
        f.write(yaml.dump(config))

    validate_config()


@pytest.mark.parametrize('config', INVALID_CONFIGS)
def test_validate_config_invalid(tmp_path, monkeypatch, config):
    monkeypatch.delenv('PANO_CLIENT_ID', raising=False)
    monkeypatch.delenv('PANO_CLIENT_SECRET', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))

    Paths.config_dir().mkdir()
    with Paths.config_file().open('w') as f:
        f.write(yaml.dump(config))

    with pytest.raises(JsonSchemaError):
        validate_config()


def test_validate_config_invalid_yaml(tmp_path, monkeypatch):
    monkeypatch.delenv('PANO_CLIENT_ID', raising=False)
    monkeypatch.delenv('PANO_CLIENT_SECRET', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))

    Paths.config_dir().mkdir()
    with Paths.config_file().open('w') as f:
        f.write('not:\nyaml')

    with pytest.raises(InvalidYamlFile):
        validate_config()


def test_validate_config_missing_file(tmp_path, monkeypatch):
    monkeypatch.delenv('PANO_CLIENT_ID', raising=False)
    monkeypatch.delenv('PANO_CLIENT_SECRET', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))

    with pytest.raises(FileMissingError):
        validate_config()


def test_validate_config_valid(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))

    Paths.config_dir().mkdir()
    with Paths.config_file().open('w') as f:
        f.write(yaml.dump(VALID_CONFIG))

    validate_config()


VALID_DATASET = {
    'dataset_slug': 'test-slug',
    'display_name': 'Test Name',
    'api_version': 'v1',
}
VALID_MODEL_MINIMAL: Dict[str, Any] = {
    'model_name': 'sf.db.schema.table1',
    'data_source': 'sf.db.schema.table1',
    'api_version': 'v1',
}
VALID_MODEL_FULL = {
    'model_name': 'sf.db.schema.table1',
    'data_source': 'sf.db.schema.table1',
    'api_version': 'v1',
    'identifiers': ['ad_id'],
    'joins': [
        {'to_model': 'sf.db.schema.table2', 'join_type': 'left', 'relationship': 'many_to_one', 'fields': ['ad_id']},
        {'to_model': 'sf.db.schema.table3', 'join_type': 'right', 'relationship': 'one_to_many', 'fields': ['ad_id']},
        {'to_model': 'sf.db.schema.table4', 'join_type': 'inner', 'relationship': 'one_to_one', 'fields': ['ad_id']},
        {'to_model': 'sf.db.schema.table5', 'join_type': 'inner', 'relationship': 'many_to_many', 'fields': ['ad_id']},
    ],
    'fields': [
        # has field_map
        {'data_reference': '"ad_id"', 'field_map': ['ad_id'], 'data_type': 'CHARACTER VARYING',},
    ],
}


INVALID_DATASETS = [
    # typo in dataset_slug
    {'dataset_slu': 'test-slug', 'display_name': 'Test Name', 'api_version': 'v1',},
    # wrong type in dataset_slug
    {'dataset_slug': 100, 'display_name': 'Test Name', 'api_version': 'v1',},
    # typo in display_name
    {'dataset_slug': 'test-slug', 'display_nam': 'Test Name', 'api_version': 'v1',},
    # wrong type in display_name
    {'dataset_slug': 'test-slug', 'display_name': 200, 'api_version': 'v1',},
    # typo in api_version
    {'dataset_slug': 'test-slug', 'display_name': 'Test Name', 'api_versio': 'v1',},
    # wrong type in api_version
    {'dataset_slug': 'test-slug', 'display_name': 'Test Name', 'api_version': 1,},
]


INVALID_MODELS = [
    # typo in model_name
    {**VALID_MODEL_MINIMAL, 'model_nae': 'sf.db.schema.table1'},
    # wrong type in model_name
    {**VALID_MODEL_MINIMAL, 'model_name': 100},
    # typo in data_source
    {**VALID_MODEL_MINIMAL, 'data_sourc': 'sf.db.schema.table1'},
    # wrong type in data_source
    {**VALID_MODEL_MINIMAL, 'data_source': 200},
    # typo in api_version
    {**VALID_MODEL_MINIMAL, 'api_versio': 'v1',},
    # wrong type in api_version
    {**VALID_MODEL_MINIMAL, 'api_version': 1,},
    # typo in identifiers
    {**VALID_MODEL_MINIMAL, 'identifirs': ['ad_id'],},
    # wrong type in identifiers
    {**VALID_MODEL_MINIMAL, 'identifiers': [100],},
    # typo in joins
    {
        **VALID_MODEL_MINIMAL,
        'jons': [
            {
                'to_model': 'sf.db.schema.table2',
                'join_type': 'left',
                'relationship': 'many_to_one',
                'fields': ['ad_id'],
            },
        ],
    },
    # invalid join type
    {
        **VALID_MODEL_MINIMAL,
        'joins': [
            {
                'to_model': 'sf.db.schema.table2',
                'join_type': 'middle',
                'relationship': 'many_to_one',
                'fields': ['ad_id'],
            },
        ],
    },
    # invalid relationship
    {
        **VALID_MODEL_MINIMAL,
        'joins': [
            {
                'to_model': 'sf.db.schema.table2',
                'join_type': 'left',
                'relationship': 'single_to_single',
                'fields': ['ad_id'],
            },
        ],
    },
    # no "to model" set
    {**VALID_MODEL_MINIMAL, 'joins': [{'join_type': 'left', 'relationship': 'many_to_one', 'fields': ['ad_id'],},],},
    # no join fields set
    {
        **VALID_MODEL_MINIMAL,
        'joins': [
            {'to_model': 'sf.db.schema.table2', 'join_type': 'left', 'relationship': 'many_to_one', 'fields': [],},
        ],
    },
    # wrong type in joins
    {**VALID_MODEL_MINIMAL, 'joins': [100],},
    # typo in fields
    {
        **VALID_MODEL_MINIMAL,
        'felds': [
            {'data_reference': '"campaign_id"', 'field_map': ['campaign_id'], 'data_type': 'CHARACTER VARYING',},
        ],
    },
    # wrong type in fields
    {**VALID_MODEL_MINIMAL, 'fields': [100],},
    # field_map not set
    {**VALID_MODEL_MINIMAL, 'fields': [{'data_reference': '"campaign_id"', 'data_type': 'CHARACTER VARYING',},],},
    # data_type not set
    {**VALID_MODEL_MINIMAL, 'fields': [{'data_reference': '"campaign_id"',},],},
    # data_reference not set
    {**VALID_MODEL_MINIMAL, 'fields': [{'data_type': 'CHARACTER VARYING',},],},
]


def test_validate_local_state_valid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    model1 = {**VALID_MODEL_MINIMAL, 'model_name': 'sf.db.schema.table1'}
    model2 = {**VALID_MODEL_MINIMAL, 'model_name': 'sf.db.schema.table2'}

    with (dataset_dir / 'test_model-1.model.yaml').open('w') as f:
        f.write(yaml.dump(model1))

    with (dataset_dir / 'test_model-2.model.yaml').open('w') as f:
        f.write(yaml.dump(model2))

    errors = validate_local_state()
    assert len(errors) == 0

    state = get_state()
    assert len(state.models) == 2
    assert len(state.data_sources) == 1


@pytest.mark.parametrize('dataset', INVALID_DATASETS)
def test_validate_local_state_invalid_dataset(tmp_path, monkeypatch, dataset):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(dataset))

    errors = validate_local_state()
    assert len(errors) == 1


@pytest.mark.parametrize('model', INVALID_MODELS)
def test_validate_local_state_invalid_models(tmp_path, monkeypatch, model):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'test_model.model.yaml').open('w') as f:
        f.write(yaml.dump(model))

    errors = validate_local_state()
    assert len(errors) == 1


def test_validate_local_state_duplicate_model_names(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'test_model-1.model.yaml').open('w') as f:
        f.write(yaml.dump(VALID_MODEL_MINIMAL))

    with (dataset_dir / 'test_model-2.model.yaml').open('w') as f:
        f.write(yaml.dump(VALID_MODEL_MINIMAL))

    errors = validate_local_state()
    assert len(errors) == 1
