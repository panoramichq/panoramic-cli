from typing import Any, Dict

import pytest
import yaml

from panoramic.cli.errors import (
    DeprecatedAttributeWarning,
    FileMissingError,
    InvalidYamlFile,
    JsonSchemaError,
    MissingFieldFileError,
    OrphanFieldFileError,
)
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
    {
        'api_versio': 'v1',
        'company_slug': 'test_company',
    },
    # wrong type in version
    {
        'api_version': 1,
        'company_slug': 'test_company',
    },
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
        {'to_model': 'sf.db.schema.table3', 'join_type': 'right', 'relationship': 'one_to_one', 'fields': ['ad_id']},
        {'to_model': 'sf.db.schema.table4', 'join_type': 'inner', 'relationship': 'one_to_one', 'fields': ['ad_id']},
        {'to_model': 'sf.db.schema.table5', 'join_type': 'inner', 'relationship': 'many_to_one', 'fields': ['ad_id']},
    ],
    'fields': [
        # has field_map
        {
            'data_reference': '"ad_id"',
            'field_map': ['ad_id'],
        },
    ],
}
VALID_FIELD_MINIMAL: Dict[str, Any] = {
    'api_version': 'v1',
    'slug': 'field_slug',
    'group': 'group',
    'display_name': 'Display name',
    'data_type': 'data_type',
    'field_type': 'field_type',
}

VALID_FIELD_FULL: Dict[str, Any] = {
    'slug': 'full_field_slug',
    'group': 'group',
    'display_name': 'Full display name',
    'data_type': 'data_type',
    'field_type': 'field_type',
    'calculation': 'calculation',
    'aggregation': {'type': 'sum'},
    'display_format': 'display_format',
    'description': 'description',
}


INVALID_DATASETS = [
    # typo in dataset_slug
    {
        'dataset_slu': 'test-slug',
        'display_name': 'Test Name',
        'api_version': 'v1',
    },
    # wrong type in dataset_slug
    {
        'dataset_slug': 100,
        'display_name': 'Test Name',
        'api_version': 'v1',
    },
    # typo in display_name
    {
        'dataset_slug': 'test-slug',
        'display_nam': 'Test Name',
        'api_version': 'v1',
    },
    # wrong type in display_name
    {
        'dataset_slug': 'test-slug',
        'display_name': 200,
        'api_version': 'v1',
    },
    # typo in api_version
    {
        'dataset_slug': 'test-slug',
        'display_name': 'Test Name',
        'api_versio': 'v1',
    },
    # wrong type in api_version
    {
        'dataset_slug': 'test-slug',
        'display_name': 'Test Name',
        'api_version': 1,
    },
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
    {
        **VALID_MODEL_MINIMAL,
        'api_versio': 'v1',
    },
    # wrong type in api_version
    {
        **VALID_MODEL_MINIMAL,
        'api_version': 1,
    },
    # typo in identifiers
    {
        **VALID_MODEL_MINIMAL,
        'identifirs': ['ad_id'],
    },
    # wrong type in identifiers
    {
        **VALID_MODEL_MINIMAL,
        'identifiers': [100],
    },
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
    {
        **VALID_MODEL_MINIMAL,
        'joins': [
            {
                'join_type': 'left',
                'relationship': 'many_to_one',
                'fields': ['ad_id'],
            },
        ],
    },
    # no join fields set
    {
        **VALID_MODEL_MINIMAL,
        'joins': [
            {
                'to_model': 'sf.db.schema.table2',
                'join_type': 'left',
                'relationship': 'many_to_one',
                'fields': [],
            },
        ],
    },
    # wrong type in joins
    {
        **VALID_MODEL_MINIMAL,
        'joins': [100],
    },
    # typo in fields
    {
        **VALID_MODEL_MINIMAL,
        'felds': [
            {
                'data_reference': '"campaign_id"',
                'field_map': ['campaign_id'],
                'data_type': 'CHARACTER VARYING',
            },
        ],
    },
    # wrong type in fields
    {
        **VALID_MODEL_MINIMAL,
        'fields': [100],
    },
    # field_map not set
    {
        **VALID_MODEL_MINIMAL,
        'fields': [
            {
                'data_reference': '"campaign_id"',
                'data_type': 'CHARACTER VARYING',
            },
        ],
    },
    # data_type not set
    {
        **VALID_MODEL_MINIMAL,
        'fields': [
            {
                'data_reference': '"campaign_id"',
            },
        ],
    },
    # data_reference not set
    {
        **VALID_MODEL_MINIMAL,
        'fields': [
            {
                'data_type': 'CHARACTER VARYING',
            },
        ],
    },
]

INVALID_FIELDS = [
    # typo in slug
    {**VALID_FIELD_MINIMAL, 'sulg': 'should_be_slug'},
    # slug not set
    {k: v for k, v in VALID_FIELD_MINIMAL.items() if k != 'slug'},
    # wrong type in slug
    {**VALID_FIELD_MINIMAL, 'slug': 123},
    # group not set
    {k: v for k, v in VALID_FIELD_MINIMAL.items() if k != 'group'},
    # field_type not set
    {k: v for k, v in VALID_FIELD_MINIMAL.items() if k != 'field_type'},
    # data_type not set
    {k: v for k, v in VALID_FIELD_MINIMAL.items() if k != 'data_type'},
    # typo in api_version
    {**VALID_FIELD_MINIMAL, 'api_versio': 'v1'},
    # wrong type in api_version
    {**VALID_FIELD_MINIMAL, 'api_version': 1},
    # wrong type in calculation
    {**VALID_FIELD_MINIMAL, 'calculation': 1},
    # wrong type in aggregation
    {**VALID_FIELD_MINIMAL, 'aggregation': 1},
    {**VALID_FIELD_MINIMAL, 'aggregation': ''},
    # Invalid aggregation object
    {**VALID_FIELD_MINIMAL, 'aggregation': {'nope': 1}},
    {**VALID_FIELD_MINIMAL, 'aggregation': {'type': 1}},
    {**VALID_FIELD_MINIMAL, 'aggregation': {'type': {}}},
    {**VALID_FIELD_MINIMAL, 'aggregation': {'type': 'sum', 'params': '1232'}},
    {**VALID_FIELD_MINIMAL, 'aggregation': {'type': 'sum', 'params': {}, 'nope': 1}},
    # wrong type in display_format
    {**VALID_FIELD_MINIMAL, 'display_format': 1},
]


def test_validate_local_state_valid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    global_fields_dir = Paths.fields_dir(tmp_path)
    global_fields_dir.mkdir()

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()
    dataset_fields_dir = Paths.fields_dir(dataset_dir)
    dataset_fields_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    model1 = {**VALID_MODEL_MINIMAL, 'model_name': 'sf.db.schema.table1'}
    model2 = {
        **VALID_MODEL_MINIMAL,
        'model_name': 'sf.db.schema.table2',
        'fields': [{'field_map': ['field_slug'], 'data_reference': '"FIELD_SLUG"'}],
    }

    with (dataset_dir / 'test_model-1.model.yaml').open('w') as f:
        f.write(yaml.dump(model1))

    with (dataset_dir / 'test_model-2.model.yaml').open('w') as f:
        f.write(yaml.dump(model2))

    with (global_fields_dir / 'company_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_FULL))

    with (dataset_fields_dir / 'first_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    errors = validate_local_state()
    assert len(errors) == 0

    state = get_state()
    assert len(state.models) == 2
    assert len(state.data_sources) == 1
    assert len(state.fields) == 2


@pytest.mark.parametrize('dataset', INVALID_DATASETS)
def test_validate_local_state_invalid_dataset(tmp_path, monkeypatch, dataset):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(dataset))

    errors = validate_local_state()
    assert len(errors) == 1


@pytest.mark.parametrize('invalid_field', INVALID_FIELDS)
def test_validate_local_state_invalid_dataset_scoped_field(tmp_path, monkeypatch, invalid_field):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    field_dir = Paths.fields_dir(dataset_dir)
    field_dir.mkdir()

    with (field_dir / 'first_field.field.yaml').open('w') as f:
        f.write(yaml.dump(invalid_field))

    errors = validate_local_state()
    assert len(errors) == 1


@pytest.mark.parametrize('invalid_field', INVALID_FIELDS)
def test_validate_local_state_duplicate_dataset_scoped_field(tmp_path, monkeypatch, invalid_field):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'test_model.model.yaml').open('w') as f:
        f.write(
            yaml.dump(
                {**VALID_MODEL_MINIMAL, 'fields': [{'field_map': ['field_slug'], 'data_reference': '"FIELD_SLUG"'}]}
            )
        )

    field_dir = Paths.fields_dir(dataset_dir)
    field_dir.mkdir()

    with (field_dir / 'first_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    with (field_dir / 'duplicate.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    errors = validate_local_state()
    assert len(errors) == 1


@pytest.mark.parametrize('invalid_field', INVALID_FIELDS)
def test_validate_local_state_invalid_company_scoped_field(tmp_path, monkeypatch, invalid_field):
    monkeypatch.chdir(tmp_path)

    global_field_dir = tmp_path / 'fields'
    global_field_dir.mkdir()

    with (global_field_dir / 'a_field.field.yaml').open('w') as f:
        f.write(yaml.dump(invalid_field))

    errors = validate_local_state()
    assert len(errors) == 1


@pytest.mark.parametrize('invalid_field', INVALID_FIELDS)
def test_validate_local_state_duplicate_company_scoped_field(tmp_path, monkeypatch, invalid_field):
    monkeypatch.chdir(tmp_path)

    global_field_dir = tmp_path / 'fields'
    global_field_dir.mkdir()

    with (global_field_dir / 'a_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_FULL))

    with (global_field_dir / 'duplicate_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_FULL))

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


def test_validate_local_state_missing_field_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'model1.model.yaml').open('w') as f:
        f.write(
            yaml.dump(
                {
                    **VALID_MODEL_MINIMAL,
                    'fields': [{'data_reference': '"COLUMN1"', 'field_map': ['field_slug', 'field_slug_2']}],
                }
            )
        )

    field_dir = Paths.fields_dir(dataset_dir)
    field_dir.mkdir()

    with (field_dir / 'field_slug.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    errors = validate_local_state()
    assert errors == [MissingFieldFileError(field_slug='field_slug_2', dataset_slug='test_dataset')]


def test_validate_local_state_deprecated_attribute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'test_model.model.yaml').open('w') as f:
        f.write(
            yaml.dump(
                {
                    **VALID_MODEL_MINIMAL,
                    'fields': [
                        {
                            'data_type': 'CHARACTER VARYING',
                            'field_map': ['field_slug'],
                            'data_reference': '"FIELD_SLUG"',
                        }
                    ],
                }
            )
        )

    Paths.fields_dir(dataset_dir).mkdir()
    with (Paths.fields_dir(dataset_dir) / 'test_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    errors = validate_local_state()
    assert errors == [DeprecatedAttributeWarning(attribute='data_type', path=dataset_dir / 'test_model.model.yaml')]


def test_validate_local_state_orphan_field_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    dataset_dir = tmp_path / 'test_dataset'
    dataset_dir.mkdir()

    with (dataset_dir / PresetFileName.DATASET_YAML.value).open('w') as f:
        f.write(yaml.dump(VALID_DATASET))

    with (dataset_dir / 'test_model.model.yaml').open('w') as f:
        f.write(
            yaml.dump(
                {
                    **VALID_MODEL_FULL,
                    'fields': [{'field_map': ['field_slug'], 'data_reference': '"FIELD_SLUG"'}],
                }
            )
        )

    Paths.fields_dir(dataset_dir).mkdir()
    with (Paths.fields_dir(dataset_dir) / 'test_field.field.yaml').open('w') as f:
        f.write(yaml.dump(VALID_FIELD_MINIMAL))

    with (Paths.fields_dir(dataset_dir) / 'calculated_field.field.yaml').open('w') as f:
        f.write(yaml.dump({**VALID_FIELD_MINIMAL, 'slug': 'calculated_slug', 'calculation': '2+2'}))

    with (Paths.fields_dir(dataset_dir) / 'orphan_field.field.yaml').open('w') as f:
        f.write(yaml.dump({**VALID_FIELD_MINIMAL, 'slug': 'orphan_slug'}))

    errors = validate_local_state()

    assert errors == [OrphanFieldFileError(field_slug='orphan_slug', dataset_slug='test_dataset')]
